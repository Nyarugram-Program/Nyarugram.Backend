namespace Botgram.Backend;

public sealed class CallExpiryService(Store store) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var now = DateTimeOffset.UtcNow;
            var expired = store.Read(db => db.Calls.Any(c =>
                (c.State == "ringing" && now - c.UpdatedAt > TimeSpan.FromSeconds(60)) ||
                (c.State == "active" && now - c.UpdatedAt > TimeSpan.FromHours(2))));
            // Store.Write expires calls and emits durable call.updated events atomically.
            if (expired) store.Write(_ => true);
        }
    }
}
