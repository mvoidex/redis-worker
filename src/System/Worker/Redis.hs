{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- | Module provides functions to work asynchrously on tasks stored in redis.
--
-- Typical usage is
--
-- @
-- foo :: Redis ()
-- foo = pushTask \"tasks\" \"task:123\" (M.fromList [\"foo\", \"bar\"])
--
-- bar :: IO ()
-- bar = do
--   conn <- connect defaultConnectInfo
--   runTask conn $ processTasks \"tasks\" \"tasks:1\" onProcess onFail
--   where
--     onProcess i m = undefined
--     onFail i = undefined
-- @
--
module System.Worker.Redis (
  -- * Types
  TaskMonad,
  MonadTask(..),
  runTask,

  -- * Strategies
  processTasks,

  -- * Tasks
  pushTaskId,
  pushTasksId,
  pushTask,

  popTaskId,
  popTask,

  reTask
  ) where

import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans
import Control.Monad.Reader
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.Map as M
import Database.Redis

newtype TaskMonad m a = TaskMonad {
  taskMonad :: ReaderT Connection m a }
    deriving (Functor, Monad, MonadIO, MonadCatchIO, MonadReader Connection, MonadTrans)

class (Monad m) => MonadTask m where
  inTask :: Redis a -> m a

instance (MonadIO m) => MonadTask (TaskMonad m) where
  inTask act = do
    conn <- ask
    liftIO $ runRedis conn act

runTask :: (MonadIO m) => Connection -> TaskMonad m a -> m a
runTask conn (TaskMonad act) = runReaderT act conn

-- | General function to process tasks
--
-- Firstly, it moves all tasks from processing-list back to task-list, then starts to pops tasks from task-list
--
processTasks :: (MonadTask m) => ByteString -> ByteString -> (ByteString -> M.Map ByteString ByteString -> m a) -> (ByteString -> m a) -> m b
processTasks tl pl process processFail = do
  reTask tl pl
  forever $ popTask tl pl 0 process processFail

-- | Push one task id
pushTaskId :: ByteString -> ByteString -> Redis ()
pushTaskId tl tid = void $ lpush tl [tid]

-- | Push several task ids
pushTasksId :: ByteString -> [ByteString] -> Redis ()
pushTasksId tl tids = void $ lpush tl tids

-- | Push one task by id and push that id to task list
pushTask :: ByteString -> ByteString -> M.Map ByteString ByteString -> Redis ()
pushTask tl tid tdata = hmset tid (M.toList tdata) >> pushTaskId tl tid

-- | Pop one task id (and push it to processing-list with auto-remove on end of callback) with block
popTaskId :: (MonadTask m) => ByteString -> ByteString -> Integer -> (ByteString -> m a) -> m a
popTaskId tl pl timeout process = do
  (Right (Just i)) <- inTask $ brpoplpush tl pl timeout
  v <- process i
  inTask $ lrem pl 0 i
  return v

-- | Pop one task like @popTaskId@, but takes one additional callback for fail on getting (hmgetall) data
popTask :: (MonadTask m) => ByteString -> ByteString -> Integer -> (ByteString -> M.Map ByteString ByteString -> m a) -> (ByteString -> m a) -> m a
popTask tl pl timeout process processFail = popTaskId tl pl timeout process' where
  process' i = do
    v <- inTask $ hgetall i
    case v of
      Left _ -> processFail i
      Right m -> process i $ M.fromList m

-- | Move all tasks from processing-list back to task-list
reTask :: (MonadTask m) => ByteString -> ByteString -> m ()
reTask tl pl = do
  (Right i) <- inTask $ rpoplpush pl tl
  case i of
    Nothing -> return ()
    Just _ -> reTask tl pl
