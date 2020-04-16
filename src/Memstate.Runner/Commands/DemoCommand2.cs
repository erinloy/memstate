using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Memstate.Configuration;
using Memstate.Host;
using Memstate.Models;
using Memstate.Models.KeyValue;

namespace Memstate.Runner.Commands
{

    /// <summary>
    /// Same as DemoCommand, but removes producer/consumer loop delays, writes to a file using wire serializer, and times the load process.
    /// </summary>
    public class DemoCommand2 : ICommand
    {
        /// <summary>
        /// DemoCommand does not terminate so this event never fires.
        /// </summary>
        public event EventHandler Done = (sender, args) => { };

        private CancellationTokenSource _cancellationTokenSource;

        private Engine<KeyValueStore<int>> _engine;

        private Task _producer;
        private Task _consumer;

        public async Task Start(string[] arguments)
        {
            _cancellationTokenSource = new CancellationTokenSource();

            var cfg = Config.Current;
            var settings = cfg.GetSettings<EngineSettings>();
            cfg.SetStorageProvider(new FileStorageProvider());
            cfg.SerializerName = "wire";

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            _engine = await Engine.Start<KeyValueStore<int>>();
            stopwatch.Stop();
            var rpms = ((float)_engine.LastRecordNumber) / ((float)stopwatch.ElapsedMilliseconds);
            Console.WriteLine($"Engine Load:{stopwatch.ElapsedMilliseconds}ms Records:{_engine.LastRecordNumber} = {rpms}/ms = {rpms * 1000F}/s");

            _producer = Task.Run(Producer);
            await Task.Delay(3000);
            _consumer = Task.Run(Consumer);
        }

        public async Task Stop()
        {
            _cancellationTokenSource.Cancel();
            await Task.WhenAll(_producer, _consumer);
            await _engine.DisposeAsync();
        }

        private async Task Producer()
        {
            var random = new Random();

            await _engine.Execute(new Set<int>("key-0", 0));

            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                for (var i = 0; i < random.Next(1, 100); i++)
                {
                    await _engine.Execute(new Set<int>($"key-{i}", i));
                }

                //await Task.Delay(random.Next(100, 5000));
            }
        }

        private async Task Consumer()
        {
            var random = new Random();
            int? version = null;
            Stopwatch stopwatch = Stopwatch.StartNew();
            long lastPrint = 0;

            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                for (var i = 0; i < random.Next(1, 100); i++)
                {
                    try
                    {
                        var node = await _engine.Execute(new Get<int>("key-0"));
                        if (version != node.Version && lastPrint + 1000 < stopwatch.ElapsedMilliseconds)
                        {
                            lastPrint = stopwatch.ElapsedMilliseconds;
                            version = node.Version;
                            var msg = $"key-0 changed. Value: {node.Value}, Version: {version}";
                            Console.WriteLine(msg);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }

                //await Task.Delay(random.Next(100, 3000));
            }
        }
    }
}