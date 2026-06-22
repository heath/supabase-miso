// dmj: usage like: runSupabase('auth','signUp', args, successCallback, errorCallback);
globalThis["runSupabase"] = function (
  namespace,
  fnName,
  args,
  successful,
  errorful
) {
  globalThis["supabase"][namespace][fnName](...args).then(({ data, error }) => {
    if (data) successful(data);
    if (error) errorful(error);
  });
};
globalThis["runSupabaseFrom"] = function (
  namespace,
  fromArg,
  fnName,
  args,
  successful,
  errorful
) {
  globalThis["supabase"][namespace]
    .from(fromArg)
    [fnName](...args)
    .then(({ data, error }) => {
      if (data) successful(data);
      if (error) errorful(error);
    });
};

// Handle update queries with filters
globalThis["runSupabaseUpdate"] = function (
  table,
  values,
  filters,
  options,
  successful,
  errorful
) {
  let query = globalThis["supabase"].from(table).update(values, options);

  // Apply each filter sequentially
  filters.forEach((filter) => {
    query = query[filter.operator](filter.column, filter.value);
  });

  query.then(({ data, error }) => {
    if (data) successful(data);
    if (error) errorful(error);
  });
};

// Helper function for running select queries with filters
globalThis.runSupabaseSelect = function (
  table,
  columns,
  args,
  successCallback,
  errorCallback
) {
  let query = globalThis.supabase.from(table).select(columns);

  const filters = args[0] || [];
  const fetchOptions = args[1] || {};

  // Apply filters
  filters.forEach((filter) => {
    query = query[filter.operator](filter.column, filter.value);
  });

  // Apply fetch options if provided
  if (fetchOptions.count) {
    query = query.count(fetchOptions.count);
  }
  if (fetchOptions.head) {
    query = query.head();
  }

  query.then((result) => {
    if (result.error) {
      errorCallback(result.error.message);
    } else {
      successCallback(result.data);
    }
  });
};

// Helper function for running delete queries with filters
globalThis.runSupabaseDelete = function (
  table,
  args,
  successCallback,
  errorCallback
) {
  let query = globalThis.supabase.from(table).delete();

  const filters = args[0] || [];
  const deleteOptions = args[1] || {};

  // Apply filters
  filters.forEach((filter) => {
    query = query[filter.operator](filter.column, filter.value);
  });

  // Apply delete options if provided
  if (deleteOptions.count) {
    query = query.count(deleteOptions.count);
  }

  query.then((result) => {
    if (result.error) {
      errorCallback(result.error.message);
    } else {
      successCallback(result.data);
    }
  });
};

globalThis["runSupabaseQuery"] = function (
  from,
  fnName,
  args,
  successful,
  errorful
) {
  globalThis["supabase"]
    ["from"](from)
    [fnName](...args)
    .then(({ data, error }) => {
      if (data) successful(data);
      if (error) errorful(error);
    });
};

// Supabase Realtime (Postgres Changes)
globalThis["subscribePostgresChanges"] = function(channelName, table, filter, changeCb, subscribedCb, errorCb) {
  var opts = { event: '*', schema: 'public', table: table };
  if (filter && filter !== '') { opts.filter = filter; }
  var channel = globalThis["supabase"]
    .channel(channelName)
    .on('postgres_changes', opts, function(payload) { changeCb(payload); })
    .subscribe(function(status) {
      if (status === 'SUBSCRIBED') subscribedCb(channel);
      else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') errorCb(status);
    });
};

globalThis["removeChannel"] = function(channel) {
  globalThis["supabase"].removeChannel(channel);
};
