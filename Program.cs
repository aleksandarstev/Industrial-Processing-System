using IndustrialProcessingSystem;

var (workers, maxQueue, jobs) = ProcessingSystem.LoadConfig("config.xml");

var system = new ProcessingSystem(maxQueue);

system.InitEvents();
system.StartReporting();


for (int i = 0; i < workers; i++)
{
    Task.Run(() => system.Worker());
}


foreach (var job in jobs)
{
    var handle = system.Submit(job);

    Task.Run(async () =>
    {
        try
        {
            var result = await handle.Result;
            var j = system.GetJob(handle.Id);
            Console.WriteLine($"GETJOB {j.Id} | {j.Type} | RESULT: {result}");
        }
        catch
        {
            Console.WriteLine($"GETJOB FAILED {handle.Id}");
        }
    });
}


for (int i = 0; i < 3; i++)
{
    Task.Run(async () =>
    {
        var rnd = new Random();
        while (true)
        {

            var type = Random.Shared.Next(2) == 0 ? JobType.IO : JobType.Prime;

            var job = new Job
            {
                Type = type,
                Payload = type == JobType.IO
                    ? $"delay:{Random.Shared.Next(500, 2000)}"
                    : $"numbers:{Random.Shared.Next(1000, 5000)},threads:{Random.Shared.Next(1, 5)}",
                Priority = Random.Shared.Next(1, 5)
            };

            try
            {
                system.Submit(job);
            }
            catch { }

            await Task.Delay(500);
        }
    });
}

Task.Run(async () =>
{
    while (true)
    {
        Console.WriteLine("\n TOP JOBS");

        var topJobs = system.GetTopJobs(5);

        foreach (var j in topJobs)
        {
            Console.WriteLine($"{j.Id} | {j.Type} | priority: {j.Priority}");
        }

        await Task.Delay(3000);
    }
});

Console.ReadLine();