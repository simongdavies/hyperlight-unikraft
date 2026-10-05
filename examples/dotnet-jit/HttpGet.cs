// HTTP GET over host networking. Run with:
//   --net --env HLUK_HTTP_GET_URL=http://HOST:PORT/path
var url = Environment.GetEnvironmentVariable("HLUK_HTTP_GET_URL");
if (string.IsNullOrWhiteSpace(url))
{
    throw new InvalidOperationException("HLUK_HTTP_GET_URL must be set explicitly");
}

using var client = new System.Net.Http.HttpClient { Timeout = TimeSpan.FromSeconds(10) };
var resp = await client.GetAsync(url);
Console.WriteLine($"Status: {(int)resp.StatusCode}");
Console.WriteLine(await resp.Content.ReadAsStringAsync());
