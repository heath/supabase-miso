-----------------------------------------------------------------------------
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
-----------------------------------------------------------------------------
module Supabase.Miso.Realtime
  ( -- * Functions
    subscribeToTable
  , removeChannel
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

-- | Remove (unsubscribe from) a Realtime channel.
removeChannel :: Channel -> IO ()
removeChannel (Channel ch) =
  void $ jsg "globalThis" # "removeChannel" $ ch
-----------------------------------------------------------------------------
