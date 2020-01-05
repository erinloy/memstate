using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace Memstate.SqlStreamStore
{
    public class SqlStreamStoreJournalWriter : BatchingJournalWriter
    {
        private readonly IStreamStore _streamStore;
        private readonly StreamId _streamId;
        private readonly ISerializer _serializer;

        public SqlStreamStoreJournalWriter(IStreamStore streamStore, StreamId streamId, ISerializer serializer)
        {
            _streamStore = streamStore;
            _streamId = streamId;
            _serializer = serializer;
        }

        public override Task DisposeAsync() => Task.CompletedTask;

        protected override void OnCommandBatch(IEnumerable<Command> commands)
        {
            var messages = commands.Select(ToNewStreamMessage).ToArray();
           var result =  _streamStore.AppendToStream(_streamId, ExpectedVersion.Any, messages )
               .GetAwaiter()
               .GetResult();
        }

        private NewStreamMessage ToNewStreamMessage(Command command)
        {
            var commandAsString = _serializer.ToString(command);
            var id = command.Id;
            return new NewStreamMessage(id, command.GetType().Name, commandAsString);
        }
    }
}