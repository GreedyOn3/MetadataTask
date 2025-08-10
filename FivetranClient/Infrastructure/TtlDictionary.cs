using System.Collections.Concurrent;
using System.Threading;

namespace FivetranClient.Infrastructure;

public class TtlDictionary<TKey, TValue> where TKey : notnull 
{
    private readonly ConcurrentDictionary<TKey, (TValue Value, DateTime Expiration)> _dictionary = new();
    private readonly ConcurrentDictionary<TKey, SemaphoreSlim> _locks = new();

    private SemaphoreSlim GetLock( TKey key ) =>
        _locks.GetOrAdd( key, _ => new SemaphoreSlim( 1, 1 ) );


    public TValue GetOrAdd(TKey key, Func<TValue> valueFactory, TimeSpan ttl) 
    {
        if( TryGetValidValue(key, out var cachedValue))
            return cachedValue;

        var semaphore = GetLock(key);
        semaphore.Wait();
        try {
            if( TryGetValidValue(key, out cachedValue))
                return cachedValue;

            var value = valueFactory();
            _dictionary[key] = (value, DateTime.UtcNow.Add(ttl));
            return value;
        } finally {
            semaphore.Release();
        }
    }

    public async Task<TValue> GetOrAddAsync(TKey key, Func<Task<TValue>> asyncFactory, TimeSpan ttl) 
    {
        if( TryGetValidValue(key, out var cachedValue))
            return cachedValue;

        var semaphore = GetLock(key);
        await semaphore.WaitAsync();
        try {
            if( TryGetValidValue(key, out cachedValue))
                return cachedValue;

            var value = await asyncFactory();
            _dictionary[key] = (value, DateTime.UtcNow.Add(ttl));
            return value;
        } finally {
            semaphore.Release();
        }
    }


    public bool TryGetValidValue(TKey key, out TValue value) 
    {
          if( _dictionary.TryGetValue(key, out var entry)) {
            if(DateTime.UtcNow < entry.Expiration) {
                value = entry.Value;
                return true;
            } else {
                RemoveEntry(key);
            }
        }

        value = default!;
        return false;
    }

    public bool RemoveEntry(TKey key) 
    {
        var removed = _dictionary.TryRemove(key, out _);

        if(_locks.TryRemove(key, out var semaphore)) 
        {
            semaphore.Dispose();
        }

        return removed;
    }
}
