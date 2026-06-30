-----------------------------------------------------------------------------
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
-----------------------------------------------------------------------------
module Supabase.Miso.Realtime
  ( -- * Functions
    subscribeToTable
  , subscribeToTableWithPresence
  , removeChannel
  , trackPresence
  , untrackPresence
    -- * Types
  , Channel (..)
  ) where
-----------------------------------------------------------------------------
import           Control.Monad (void)
-----------------------------------------------------------------------------
import           Miso.JSON (Value, FromJSON, fromJSON, Result(..))
import           Miso.String (MisoString, ms)
import           Miso.DSL (JSVal, toJSVal, fromJSValUnchecked, jsg, (#))
import           Miso hiding ((<#))
import           Miso.FFI (asyncCallback1)
-----------------------------------------------------------------------------
import           Supabase.Miso.Core (errorCallback)
-----------------------------------------------------------------------------

-- | Opaque handle to a Supabase Realtime channel.
newtype Channel = Channel { unChannel :: JSVal }

-- | Subscribe to Postgres Changes on a table.
--
-- @channelName@ is an arbitrary name for the channel.
-- @table@ is the Postgres table name.
-- @filter@ is an optional PostgREST-style filter (e.g. @"id=eq.some-uuid"@),
-- pass @""@ for no filter.
--
-- The @changeCb@ fires with the raw Postgres Changes payload (a @Value@)
-- on every INSERT, UPDATE, or DELETE.
-- The @subscribedCb@ fires once with the @Channel@ handle when the
-- subscription is confirmed.
-- The @errorCb@ fires if the subscription fails.
subscribeToTable
  :: MisoString
  -- ^ Channel name
  -> MisoString
  -- ^ Table name
  -> MisoString
  -- ^ Filter (PostgREST format, e.g. "id=eq.xxx"; "" for none)
  -> (Value -> action)
  -- ^ Postgres Changes payload callback
  -> (Channel -> action)
  -- ^ Subscribed callback (receives the channel handle)
  -> (MisoString -> action)
  -- ^ Error callback
  -> Effect parent props model action
subscribeToTable channelName table filter changeCb subscribedCb errorCb =
  withSink $ \sink -> do
    changeFn <- Function <$> asyncCallback1 (\jsval -> do
      val <- fromJSValUnchecked jsval
      sink (changeCb val))
    subscribedFn <- Function <$> asyncCallback1 (\jsval ->
      sink (subscribedCb (Channel jsval)))
    errorFn <- errorCallback sink errorCb
    channelNameVal <- toJSVal channelName
    tableVal <- toJSVal table
    filterVal <- toJSVal filter
    void $ jsg "globalThis" # "subscribePostgresChanges" $
      (channelNameVal, tableVal, filterVal, changeFn, subscribedFn, errorFn)

-- | Subscribe to Postgres Changes on a table with Presence tracking.
--
-- Like 'subscribeToTable' but also registers a presence @sync@ callback
-- that fires whenever the set of tracked presences changes. The callback
-- receives the result of @channel.presenceState()@ as a @Value@.
--
-- Uses the @[JSVal]@ list pattern to pass 7 arguments (Miso's @ToArgs@
-- tuple instances max out at 6).
subscribeToTableWithPresence
  :: MisoString
  -- ^ Channel name
  -> MisoString
  -- ^ Table name
  -> MisoString
  -- ^ Filter (PostgREST format, e.g. "id=eq.xxx"; "" for none)
  -> (Value -> action)
  -- ^ Postgres Changes payload callback
  -> (Value -> action)
  -- ^ Presence sync callback (receives presenceState())
  -> (Channel -> action)
  -- ^ Subscribed callback (receives the channel handle)
  -> (MisoString -> action)
  -- ^ Error callback
  -> Effect parent props model action
subscribeToTableWithPresence channelName table filter changeCb presenceSyncCb subscribedCb errorCb =
  withSink $ \sink -> do
    changeFn <- asyncCallback1 (\jsval -> do
      val <- fromJSValUnchecked jsval
      sink (changeCb val))
    presenceSyncFn <- asyncCallback1 (\jsval -> do
      val <- fromJSValUnchecked jsval
      sink (presenceSyncCb val))
    subscribedFn <- asyncCallback1 (\jsval ->
      sink (subscribedCb (Channel jsval)))
    (Function errorFn) <- errorCallback sink errorCb
    channelNameVal <- toJSVal channelName
    tableVal <- toJSVal table
    filterVal <- toJSVal filter
    void $ jsg "globalThis" # "subscribePostgresChangesWithPresence" $
      ([channelNameVal, tableVal, filterVal, changeFn, presenceSyncFn, subscribedFn, errorFn] :: [JSVal])

-- | Announce presence on a channel.
--
-- Call this after subscribing to start tracking. The payload is an
-- arbitrary JSON object visible to all subscribers (e.g. user id,
-- display name).
trackPresence :: Channel -> Value -> IO ()
trackPresence (Channel ch) payload = do
  payloadVal <- toJSVal payload
  void $ jsg "globalThis" # "trackPresence" $ (ch, payloadVal)

-- | Stop tracking presence on a channel.
untrackPresence :: Channel -> IO ()
untrackPresence (Channel ch) =
  void $ jsg "globalThis" # "untrackPresence" $ ch

-- | Remove (unsubscribe from) a Realtime channel.
removeChannel :: Channel -> IO ()
removeChannel (Channel ch) =
  void $ jsg "globalThis" # "removeChannel" $ ch
-----------------------------------------------------------------------------
