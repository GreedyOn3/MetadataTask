using System.Net;
using FivetranClient.Infrastructure;

namespace FivetranClient;

public class HttpRequestHandler
{
    private readonly HttpClient _client;
    private readonly SemaphoreSlim? _semaphore;
    private readonly object _lock = new();
    private DateTime _retryAfterTime = DateTime.UtcNow;
    private static readonly TtlDictionary<string, HttpResponseMessage> _responseCache = new();

    /// <summary>
    /// Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
    /// Also caches responses to avoid unnecessary network calls.
    /// </summary>
    /// <remarks>
    /// Set <paramref name="maxConcurrentRequests"/> to 0 to disable concurrency limit.
    /// </remarks>
    public HttpRequestHandler(HttpClient client, ushort maxConcurrentRequests = 0)
    {
        _client = client;
        if (maxConcurrentRequests > 0)
        {
            _semaphore = new SemaphoreSlim(maxConcurrentRequests, maxConcurrentRequests);
        }
    }

    public async Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
    {   
        return await _responseCache.GetOrAddAsync(
            url,
            () => _GetAsync(url, cancellationToken),
            TimeSpan.FromMinutes(60));
    }

    private async Task<HttpResponseMessage> _GetAsync(string url, CancellationToken cancellationToken)
    {
        if (_semaphore is not null)
        {
            await _semaphore.WaitAsync(cancellationToken);
        }

        try {
            TimeSpan timeToWait;
            lock( _lock ) {
                timeToWait = _retryAfterTime - DateTime.UtcNow;
            }

            if( timeToWait > TimeSpan.Zero ) {
                await Task.Delay( timeToWait, cancellationToken );
            }

            cancellationToken.ThrowIfCancellationRequested();

            var response = await _client.GetAsync( new Uri( url, UriKind.Relative ), cancellationToken );
            if( response.StatusCode is HttpStatusCode.TooManyRequests ) {
                var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds( 60 );

                lock( _lock ) {
                    _retryAfterTime = DateTime.UtcNow.Add( retryAfter );
                }

                // new request will wait for the specified time before retrying
                return await _GetAsync( url, cancellationToken );
            }
            response.EnsureSuccessStatusCode();


            return response;

        } catch( TaskCanceledException ) when( !cancellationToken.IsCancellationRequested ) {
            // Timeout
            throw new TimeoutException( "The request has timed out." );
        } catch( OperationCanceledException ) when( cancellationToken.IsCancellationRequested ) {
            // Explicit cancellation
            throw;
        } catch( HttpRequestException httpEx ) {
            // Network-related issue
            throw new Exception( $"HTTP request failed: {httpEx.Message}", httpEx );
        } catch( Exception ex ) {
            // Unknown/unexpected issue
            throw new Exception( $"Unexpected error occurred: {ex.Message}", ex );
        }
         finally {
            _semaphore?.Release();
        }
  
    }
}
