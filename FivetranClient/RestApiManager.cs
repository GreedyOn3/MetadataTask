using System.Net;
using FivetranClient.Fetchers;
using FivetranClient.Infrastructure;
using FivetranClient.Models;

namespace FivetranClient;

public class RestApiManager( HttpRequestHandler requestHandler ) : IDisposable {
    private readonly PaginatedFetcher _paginatedFetcher = new( requestHandler ?? throw new ArgumentNullException( nameof( requestHandler ) ) );
    private readonly NonPaginatedFetcher _nonPaginatedFetcher = new( requestHandler ?? throw new ArgumentNullException( nameof( requestHandler ) ) );
    // Indicates whether this instance owns the HttpClient and should dispose it.
    private readonly HttpClient? _createdClient = null;
    private static readonly Uri ApiBaseUrl = new("https://api.fivetran.com/v1/");

    public RestApiManager(string apiKey, string apiSecret, TimeSpan timeout)
        : this(ApiBaseUrl, apiKey, apiSecret, timeout)
    {
    }

    public RestApiManager(Uri baseUrl, string apiKey, string apiSecret, TimeSpan timeout)
        : this(new FivetranHttpClient(baseUrl, apiKey, apiSecret, timeout), true)
    {
    }

    public RestApiManager(HttpClient client)
       : this(client, false) {
    }

    private RestApiManager(HttpClient client, bool ownsClient)
        : this(new HttpRequestHandler(ValidateHttpClient(client))) 
    {
        if(ownsClient) 
        {
            _createdClient = client;
        }
    }


    public IAsyncEnumerable<Group> GetGroupsAsync(CancellationToken cancellationToken) 
    {
        const string endpointPath = "groups";
        return _paginatedFetcher.FetchItemsAsync<Group>(endpointPath, cancellationToken);
    }

    public IAsyncEnumerable<Connector> GetConnectorsAsync(string groupId, CancellationToken cancellationToken) 
    {
        if(string.IsNullOrWhiteSpace(groupId))
            throw new ArgumentException( "Group ID cannot be null or empty.", nameof(groupId));

        var endpointPath = $"groups/{WebUtility.UrlEncode(groupId)}/connectors";
        return _paginatedFetcher.FetchItemsAsync<Connector>(endpointPath, cancellationToken);
    }

    public async Task<DataSchemas?> GetConnectorDataSchemasAsync(string connectorId, CancellationToken cancellationToken) 
    {
        if( string.IsNullOrWhiteSpace(connectorId))
            throw new ArgumentException( "Connector ID cannot be null or empty.", nameof(connectorId));

        var endpointPath = $"connectors/{WebUtility.UrlEncode(connectorId)}/schemas";
        return await _nonPaginatedFetcher.FetchAsync<DataSchemas>(endpointPath, cancellationToken);
    }

   

    private static HttpClient ValidateHttpClient(HttpClient client) 
    {
        ArgumentNullException.ThrowIfNull(client);

        if( client.BaseAddress == null || !client.BaseAddress.IsAbsoluteUri )
            throw new ArgumentException( "HttpClient.BaseAddress must be a non-null absolute URI.", nameof( client ));

        if( client.BaseAddress != ApiBaseUrl)
            throw new ArgumentException( "Incorrect HttpClient.BaseAddress.", nameof( client ) );

        if( client.Timeout <= TimeSpan.Zero )
            throw new ArgumentException( "HttpClient.Timeout must be greater than zero.", nameof(client));

        if( client.DefaultRequestHeaders.Authorization == null ||
       !string.Equals( client.DefaultRequestHeaders.Authorization.Scheme, "Basic", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException( "HttpClient must have a Basic Authorization header set.", nameof(client));



        return client;
    }

    public void Dispose() 
    {
        _createdClient?.Dispose();
        GC.SuppressFinalize(this);
    }
}
