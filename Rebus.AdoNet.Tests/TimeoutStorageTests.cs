using System;
using System.IO;

using Common.Logging;
using NUnit.Framework;

namespace Rebus.AdoNet
{
	[TestFixtureSource(typeof(DatabaseFixtureBase), nameof(DatabaseFixtureBase.ConnectionSources))]
	public class TimeoutStorageTests : DatabaseFixtureBase
	{
		private static readonly ILog _Log = LogManager.GetLogger<SagaPersisterTests>();
		private const string TIMEOUTS_TABLE = "timeouts";

		private AdoNetConnectionFactory _factory;
		private AdoNetTimeoutStorage _storage;

		public TimeoutStorageTests(string provider, string connectionString)
			: base(provider, connectionString)
		{
		}

		[OneTimeSetUp]
		public void OneTimeSetup()
		{
			_factory = new AdoNetConnectionFactory(ConnectionString, ProviderName);
		}

		[SetUp]
		public new void SetUp()
		{
			DropTable(TIMEOUTS_TABLE);

			_storage = new AdoNetTimeoutStorage(_factory, TIMEOUTS_TABLE);
		}

		[Test]
		public void CanCreateStorageTableAutomatically()
		{
			_storage.EnsureTableIsCreated();

			var tableNames = GetTableNames();
			Assert.That(tableNames, Contains.Item(TIMEOUTS_TABLE));
		}

		[Test]
		public void DoesntDoAnythingIfTheTableAlreadyExists()
		{
			ExecuteCommand(@"CREATE TABLE """ + TIMEOUTS_TABLE + @""" (""id"" INT NOT NULL)");

			_storage.EnsureTableIsCreated();
			_storage.EnsureTableIsCreated();
			_storage.EnsureTableIsCreated();
		}
	}
}