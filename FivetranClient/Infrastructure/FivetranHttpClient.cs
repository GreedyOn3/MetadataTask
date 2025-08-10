using System.Net.Http.Headers;
using System.Text;

namespace FivetranClient.Infrastructure;

public class FivetranHttpClient : HttpClient
{
    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret, TimeSpan timeout)
    {
        try {
            ValidateBaseUrl(baseAddress);
            ValidateApiKey(apiKey);
            ValidateApiSecret(apiSecret);
            ValidateTimeout(timeout);

            this.DefaultRequestHeaders.Clear();
            this.BaseAddress = baseAddress;
            this.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Basic", CalculateToken(apiKey, apiSecret));
            this.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            // we need to set Agent Header because otherwise sometimes it may be blocked by the server
            // see: https://repost.aws/knowledge-center/waf-block-http-requests-no-user-agent
            this.DefaultRequestHeaders.UserAgent.ParseAdd("aseduigbn");
            this.Timeout = timeout;
        }catch(Exception ex) {
            throw new InvalidOperationException( $"Failed to initialize FivetranHttpClient due to invalid parameters or configuration => {ex.Message}", ex );
        }
    }

    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret)
        : this(baseAddress, apiKey, apiSecret, TimeSpan.FromSeconds(40))
    {
    }

    private static void ValidateBaseUrl(Uri? baseUrl) 
    {
        ArgumentNullException.ThrowIfNull(baseUrl);
        if(!baseUrl.IsAbsoluteUri)
            throw new ArgumentException( "Base URL must be an absolute URI.", nameof(baseUrl));
    }

    private static void ValidateApiKey(string? apiKey)
    {
        if( string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException( "API key cannot be null or empty.", nameof(apiKey) );
    }

    private static void ValidateApiSecret(string? apiSecret)
    {
        if( string.IsNullOrWhiteSpace(apiSecret))
            throw new ArgumentException( "API secret cannot be null or empty.", nameof(apiSecret ) );
    }

    private static void ValidateTimeout(TimeSpan timeout)
    {
        if( timeout.Ticks <= 0 )
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be a positive value" );
    }

    private static string CalculateToken(string apiKey, string apiSecret)
    {
        return Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:{apiSecret}"));
    }
}
