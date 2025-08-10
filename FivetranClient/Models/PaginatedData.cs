namespace FivetranClient.Models;

public class PaginatedData<T>
{
    public List<T>? Items { get; set; }
    public string? NextCursor { get; set; }
}
