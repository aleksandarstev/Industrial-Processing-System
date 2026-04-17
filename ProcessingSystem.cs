using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace IndustrialProcessingSystem
{
    public class ProcessingSystem
    {
        private readonly PriorityQueue<Job, int> _queue = new();
        private readonly object _lock = new();
        private readonly SemaphoreSlim _signal = new(0);
        private readonly Dictionary<Guid, Job> _jobs = new();
        private readonly HashSet<Guid> _processed = new();
        private readonly Dictionary<Guid,TaskCompletionSource<int>> _tcsMap = new();

        public event Action<Job, int> JobCompleted;
        public event Action<Job> JobFailed;


        private readonly int _maxQueueSize;


        private readonly List<(Job job, bool success, int duration)> _history = new();

        private readonly object _logLock = new();


        public ProcessingSystem(int maxQueueSize)
        {
            _maxQueueSize = maxQueueSize;
        }

        public JobHandle Submit(Job job)
        {
            lock (_lock)
            {

                if (_queue.Count >= _maxQueueSize)
                    throw new Exception("Queue full");

                if (_processed.Contains(job.Id) || _jobs.ContainsKey(job.Id))
                    throw new Exception("Duplicate job");


                _queue.Enqueue(job, job.Priority);

                var tcs = new TaskCompletionSource<int>();

                _jobs[job.Id] = job;
                _tcsMap[job.Id] = tcs;

                _signal.Release();

                return new JobHandle
                {
                    Id = job.Id,
                    Result = tcs.Task
                };
            }
        }

        private async Task Process(Job job)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            int attempts = 0;

            while (attempts < 3)
            {
                attempts++;
                try
                {
                    var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    var task = Task.Run(() => Execute(job));

                    if (await Task.WhenAny(task, Task.Delay(2000)) != task)
                        throw new Exception("Timeout");

                    var result = await task;
                    _tcsMap[job.Id].SetResult(result);
                    sw.Stop();

                    lock (_lock)
                    {
                        _history.Add((job, true, (int)sw.ElapsedMilliseconds));
                    }


                    lock (_lock)
                    {
                        _processed.Add(job.Id);
                    }

                    var completed = JobCompleted;
                    completed?.Invoke(job, result);

                    return;
                }
                catch
                {
                    if (attempts == 3)
                    {

                        lock (_lock)
                        {
                            _history.Add((job, false, 2000));
                        }

                        _tcsMap[job.Id].SetException(new Exception("ABORT"));
                        JobFailed?.Invoke(job);
                    }
                }
            }
        }

        public async Task Worker()
        {
            while (true)
            {
                Job job;
                await _signal.WaitAsync();

                lock (_lock)
                {
                    job = _queue.Dequeue();
                }

                await Process(job);
            }
        }


        private int Execute(Job job)
        {
            if (job.Type == JobType.Prime)
            {
                var parts = job.Payload.Split(',');

                int n = int.Parse(parts[0].Split(':')[1].Replace("_", ""));

                int threads = int.Parse(parts[1].Split(':')[1]);

                threads = Math.Clamp(threads, 1, 8);

                int count = 0;
                object lockObj = new();

                Parallel.For(2, n, new ParallelOptions
                {
                    MaxDegreeOfParallelism = threads
                },
                i =>
                {
                    if (IsPrime(i))
                    {
                        lock (lockObj)
                            count++;
                    }
                });
                return count;
            }
            else
            {
                int delay = int.Parse(job.Payload.Split(':')[1].Replace("_", ""));

                Thread.Sleep(delay);

                return Random.Shared.Next(0, 100);
            }
        }


        private bool IsPrime(int n)
        {
            if (n < 2) return false;

            for (int i = 2; i * i <= n; i++)
                if (n % i == 0) return false;

            return true;
        }



        public void InitEvents()
        {
            JobCompleted += async (job, result) =>
            {
                lock (_logLock)
                {
                    File.AppendAllText("log.txt", $"{DateTime.Now} SUCCESS {job.Id} {result}\n");
                }
            };

            JobFailed += async (job) =>
            {
                lock (_logLock)
                {
                    File.AppendAllText("log.txt", $"{DateTime.Now} FAILED {job.Id} ABORT\n");
                }
            };
        }


        public IEnumerable<Job> GetTopJobs(int n)
        {
            lock (_lock)
            {
                return _queue.UnorderedItems.OrderBy(x => x.Priority).Take(n).Select(x => x.Element).ToList();
            }
        }

        public Job GetJob(Guid id)
        {
            lock (_lock)
            {
                return _jobs.ContainsKey(id) ? _jobs[id] : null;
            }
        }


        public void StartReporting()
        {
            var timer = new Timer(_ => GenerateReport(), null, 0, 60000);
        }


        private int _reportIndex = 0;

        private void GenerateReport()
        {
            lock (_lock)
            {

                if (_history.Count == 0) return;

                var report = _history.GroupBy(x => x.job.Type)
                    .Select(g => new
                    {
                        Type = g.Key,
                        Count = g.Count(),
                        AvgTime = g.Average(x => x.duration),
                        Failed = g.Count(x => !x.success)
                    });

                var xml = new XElement("Report",
                    report.Select(r =>
                        new XElement("JobType",
                            new XAttribute("Type", r.Type),
                            new XElement("Count", r.Count),
                            new XElement("AvgTime", r.AvgTime),
                            new XElement("Failed", r.Failed)
                        )
                    )
                );

                File.WriteAllText($"report_{_reportIndex % 10}.xml", xml.ToString());
                _reportIndex++;
            }
        }



        public static (int workers, int maxQueue, List<Job> jobs) LoadConfig(string path)
         {
            var doc = XDocument.Load(path);

            int workers = int.Parse(doc.Root.Element("WorkerCount").Value);
            int maxQueue = int.Parse(doc.Root.Element("MaxQueueSize").Value);
            
            var jobs = doc.Root.Element("Jobs").Elements("Job").Select(x => new Job
                  {
                Type = Enum.Parse<JobType>(x.Attribute("Type").Value),
                Payload = x.Attribute("Payload").Value,
                Priority = int.Parse(x.Attribute("Priority").Value)}).ToList();

            return (workers, maxQueue, jobs);
         }

}
}
