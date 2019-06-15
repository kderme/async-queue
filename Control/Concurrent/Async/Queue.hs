{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveAnyClass #-}


module Control.Concurrent.Async.Queue where
import Control.Concurrent (ThreadId, forkOS, threadDelay)
import Control.Concurrent.STM
import Control.Concurrent.STM.TBQueue
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TMVar

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad

-- This doesn't really belong here.
import Data.Pool

data AsyncQueue r = AsyncQueue
  { asyncThreadId :: !ThreadId
                  -- ^ Returns the 'ThreadId' of the thread running

  , _asyncQueue   :: TBQueue (AsyncAction r)
                  -- ^ A queue which can be used to send actions
                  -- to the thread.

  , _serves       :: (TVar Bool)

  , _resource     :: r
                  -- ^ the resource. We may not want to give AsyncQueue
                  -- access to the resource and allow only the forked thread
                  -- to have access.
  }

instance Eq (AsyncQueue a) where
    a == b = asyncThreadId a == asyncThreadId b

data AsyncAction r = forall a. AsyncAction (r -> IO a) (TMVar (Either SomeException a))

-- | Create a thread which initiates a resource and waits to serve
-- users with this resource. This function blocks until the resource is created
-- or rethrows any exception if the creation failed.
asyncQueue :: Int -> IO r -> IO (AsyncQueue r)
asyncQueue = asyncQueueUsing forkIO

asyncQueueBound :: Int -> IO r -> IO (AsyncQueue r)
asyncQueueBound = asyncQueueUsing forkOS

asyncQueueOn :: Int -> Int -> IO r -> IO (AsyncQueue r)
asyncQueueOn cpu = asyncQueueUsing (forkOn cpu)

asyncQueueUsing :: (IO () -> IO ThreadId) -> Int
                -> IO r -> IO (AsyncQueue r)
asyncQueueUsing doFork queueSize createResource = do
    resVar :: TMVar (Either SomeException r) <- newEmptyTMVarIO
    servesVar <- newTVarIO True
    queue <- newTBQueueIO $ fromIntegral queueSize
    t <- doFork $ do
            flip catch (\e -> atomically $ putTMVar resVar $ Left e) $ do
                r <- createResource
                atomically $ putTMVar resVar $ Right r
                serve queue servesVar r
    res <- atomically $ takeTMVar resVar -- we don't need readTMVar, since this TMVar will be shortly gone.
    case res of
        Left e -> throwIO e
        Right r -> return $ AsyncQueue t queue servesVar r

serve :: TBQueue (AsyncAction r) -> (TVar Bool) -> r -> IO ()
serve queue serves resource = go
    where
        go = do
            AsyncAction action mvar <- atomically $ readTBQueue queue
            ret <- try $ action resource
            atomically $ putTMVar mvar ret
            servesNow <- atomically $ readTVar serves
            if servesNow then go else return ()

withAsyncQueue :: Int -> IO r -> (AsyncQueue r -> IO b) -> (r -> IO ()) -> IO b
withAsyncQueue = withAsyncQueueUsing forkIO

-- | Like 'withAsync' but uses 'forkOS' internally.
withAsyncQueueBound :: Int -> IO r -> (AsyncQueue r -> IO b) -> (r -> IO ()) -> IO b
withAsyncQueueBound = withAsyncQueueUsing forkOS

-- | Like 'withAsync' but uses 'forkOn' internally.
withAsyncQueueOn :: Int -> Int -> IO r -> (AsyncQueue r -> IO b) -> (r -> IO ()) -> IO b
withAsyncQueueOn = withAsyncQueueUsing . forkOn

-- A safer combinator which makes sure that the queue is closed and the resource is
-- terminated, even if the main action gets an exception.
withAsyncQueueUsing :: (IO () -> IO ThreadId) -> Int
               -> IO r -> (AsyncQueue r -> IO b) -> (r -> IO ()) -> IO b
withAsyncQueueUsing doFork queueSize createResource f destroy =
    bracketOnError (asyncQueueUsing doFork queueSize createResource)
                   (\q -> waitQueue True q destroy)
                   f

-- | Send an action to the os bound thread.
waitQueue :: Bool -> AsyncQueue r -> (r -> IO a) -> IO a
waitQueue lastCommand async action = do
    resp <- newEmptyTMVarIO
    atomically $ do
        serves <- readTVar $ _serves async
        if serves then writeTBQueue (_asyncQueue async) $ AsyncAction action resp
        else throwSTM DoesntServeError
    ret <- atomically $ takeTMVar resp
    case ret of
        Left e -> throwIO e
        Right a -> return a

-- This doesn't belong here
data AsyncWithPool r = AsyncWithPool {
      queue :: AsyncQueue r
    , pool  :: Pool r
    }

-- This doesn't belong here
mkAsyncWithPool :: AsyncQueue r -> Pool r -> AsyncWithPool r
mkAsyncWithPool q p = AsyncWithPool q p

data DoesntServeError = DoesntServeError
        deriving (Show, Exception)
