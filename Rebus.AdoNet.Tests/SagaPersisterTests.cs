using System;
using System.IO;
using System.Data;

using Common.Logging;
using NUnit.Framework;

namespace Rebus.AdoNet
{
	[TestFixture]
	public class SagaPersisterTests : DatabaseFixtureBase
	{
		#region Inner Types
		class PieceOfSagaData : ISagaData
		{
			public PieceOfSagaData()
			{
				Id = Guid.NewGuid();
			}
			public Guid Id { get; set; }
			public int Revision { get; set; }
			public string SomeProperty { get; set; }
			public string AnotherProperty { get; set; }
		}

		class SomeSagaData : ISagaData
		{
			public Guid Id { get; set; }
			public int Revision { get; set; }
			public string JustSomething { get; set; }
		}
		#endregion

		private static readonly ILog _Log = LogManager.GetLogger<SagaPersisterTests>();

		protected const string SagaTableName = "Sagas";
		protected const string SagaIndexTableName = "SagasIndex";

		#region UserProvided Connection/Transaction Helpers

		private IDbConnection _connection = null;
		private IDbTransaction _transaction = null;

		private ConnectionHolder GetOrCreateConnection()
		{
			if (_connection != null)
			{
				return _transaction == null
					? ConnectionHolder.ForNonTransactionalWork(_connection, Dialect)
					: ConnectionHolder.ForTransactionalWork(_connection, Dialect, _transaction);
			}

			var connection = Factory.CreateConnection();
			connection.ConnectionString = ConnectionString;
			connection.Open();
			_connection = connection;

			return ConnectionHolder.ForNonTransactionalWork(connection, Dialect);
		}

		private void BeginTransaction()
		{
			if (_transaction != null)
			{
				throw new InvalidOperationException("Cannot begin new transaction when a transaction has already been started!");
			}

			_transaction = GetOrCreateConnection().Connection.BeginTransaction();
		}

		private void CommitTransaction()
		{
			if (_transaction == null)
			{
				throw new InvalidOperationException("Cannot commit transaction when no transaction has been started!");
			}

			_transaction.Commit();
			_transaction = null;
		}

		#endregion

		protected override void OnSetUp()
		{
			DropSagaTables();
		}

		protected void DropSagaTables()
		{
			try
			{
				DropTable(SagaTableName);
			}
			catch
			{
			}

			try
			{
				DropTable(SagaIndexTableName);
			}
			catch
			{
			}
		}

		protected AdoNetSagaPersister CreatePersister(bool userProvidedConnection = false, bool createTables = false)
		{
			var result = new AdoNetSagaPersister(ConnectionString, ProviderName, SagaIndexTableName, SagaTableName);
			if (createTables) result.EnsureTablesAreCreated();
			return result;
		}

		#region Basic Saga Persister tests
		[Test]
		public void WhenIgnoringNullProperties_DoesNotSaveNullPropertiesOnUpdate()
		{
			var persister = CreatePersister(createTables: true);

			persister.DoNotIndexNullProperties();

			const string correlationProperty1 = "correlation property 1";
			const string correlationProperty2 = "correlation property 2";
			var correlationPropertyPaths = new[]
			{
				Reflect.Path<PieceOfSagaData>(s => s.SomeProperty),
				Reflect.Path<PieceOfSagaData>(s => s.AnotherProperty)
			};

			var firstPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData { SomeProperty = correlationProperty1, AnotherProperty = "random12423" };
			var nextPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData { SomeProperty = correlationProperty2, AnotherProperty = "random38791387" };

			persister.Insert(firstPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);
			persister.Insert(nextPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			var firstPiece = persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty1);
			firstPiece.AnotherProperty = null;
			persister.Update(firstPiece, correlationPropertyPaths);

			var nextPiece = persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty2);
			nextPiece.AnotherProperty = null;
			persister.Update(nextPiece, correlationPropertyPaths);
		}

		[Test]
		public void WhenIgnoringNullProperties_DoesNotSaveNullPropertiesOnInsert()
		{
			var persister = CreatePersister(createTables: true);

			persister.DoNotIndexNullProperties();

			const string correlationProperty1 = "correlation property 1";
			const string correlationProperty2 = "correlation property 2";
			var correlationPropertyPaths = new[]
			{
				Reflect.Path<PieceOfSagaData>(s => s.SomeProperty),
				Reflect.Path<PieceOfSagaData>(s => s.AnotherProperty)
			};

			var firstPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData
			{
				SomeProperty = correlationProperty1
			};

			var nextPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData
			{
				SomeProperty = correlationProperty2
			};

			var firstId = firstPieceOfSagaDataWithNullValueOnProperty.Id;
			var nextId = nextPieceOfSagaDataWithNullValueOnProperty.Id;

			persister.Insert(firstPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			// must not throw:
			persister.Insert(nextPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			var firstPiece = persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty1);
			var nextPiece = persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty2);

			Assert.That(firstPiece.Id, Is.EqualTo(firstId));
			Assert.That(nextPiece.Id, Is.EqualTo(nextId));
		}

		[Test]
		public void CanCreateSagaTablesAutomatically()
		{
			// arrange

			// act
			CreatePersister().EnsureTablesAreCreated();

			// assert
			var existingTables = GetTableNames();
			Assert.That(existingTables, Contains.Item(SagaIndexTableName));
			Assert.That(existingTables, Contains.Item(SagaTableName));
		}

		[Test]
		public void DoesntDoAnythingIfTheTablesAreAlreadyThere()
		{
			// arrange
			var persister = CreatePersister();
			ExecuteCommand(@"CREATE TABLE """ + SagaTableName + @""" (""id"" INT NOT NULL)");
			ExecuteCommand(@"CREATE TABLE """ + SagaIndexTableName + @""" (""id"" INT NOT NULL)");

			// act

			// assert
			persister.EnsureTablesAreCreated();
			persister.EnsureTablesAreCreated();
			persister.EnsureTablesAreCreated();
		}

		#endregion

		#region User Provider Connection tests

		[Test]
		public void WorksWithUserProvidedConnectionWithStartedTransaction()
		{
			// arrange
			var persister = CreatePersister(userProvidedConnection: true, createTables: true);
			var sagaId = Guid.NewGuid();
			var sagaData = new SomeSagaData { JustSomething = "hey!", Id = sagaId };

			// act
			BeginTransaction();

			// assert
			persister.Insert(sagaData, new string[0]);

			CommitTransaction();
		}

		[Test]
		public void WorksWithUserProvidedConnectionWithoutStartedTransaction()
		{
			var persister = CreatePersister(userProvidedConnection: true, createTables: true);
			var sagaId = Guid.NewGuid();
			var sagaData = new SomeSagaData { JustSomething = "hey!", Id = sagaId };

			persister.Insert(sagaData, new string[0]);
		}

		[Test]
		public void CanCreateSagaTablesAutomaticallyWithUserProvidedConnection()
		{
			var persister = CreatePersister(userProvidedConnection: true, createTables: true);

			var existingTables = GetTableNames();
			Assert.That(existingTables, Contains.Item(SagaIndexTableName));
			Assert.That(existingTables, Contains.Item(SagaTableName));
		}

		#endregion

	}
}