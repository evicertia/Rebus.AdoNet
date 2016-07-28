using System;
using System.IO;
using System.Data;
using System.Collections.Generic;
using System.Reflection;

using Common.Logging;
using NUnit.Framework;

using Rebus;
using Rebus.Bus;
using Rebus.Shared;

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

		class SomePieceOfSagaData : ISagaData
		{
			public Guid Id { get; set; }
			public int Revision { get; set; }
			public string PropertyThatCanBeNull { get; set; }
			public string SomeValueWeCanRecognize { get; set; }
		}

		class SagaDataWithNestedElement : ISagaData
		{
			public Guid Id { get; set; }
			public int Revision { get; set; }
			public ThisOneIsNested ThisOneIsNested { get; set; }
		}

		class ThisOneIsNested
		{
			public string SomeString { get; set; }
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

		#region Fake Message Context Helpers

		private IMessageContext _messageContext = null;
		private static MethodInfo _MessageContextEstablishMethod = typeof(MessageContext)
				.GetMethod("Establish", BindingFlags.Static | BindingFlags.NonPublic, null, new[] { typeof(IDictionary<string, object>) }, null);
		private static Func<IDictionary<string, object>, MessageContext> _MessageContextEstablishAccessor =
			x => (MessageContext)_MessageContextEstablishMethod.Invoke(null, new object[] { x });

		protected IDisposable EstablishMessageContext()
		{
			var headers = new Dictionary<string, object>
				{
					{Headers.ReturnAddress, "none"},
					{Headers.MessageId, "just_some_message_id"},
				};

			var result = new NoTransaction();
			Disposables.TrackDisposable(result);
			_messageContext = _MessageContextEstablishAccessor(headers);

			return result;
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
			var result = userProvidedConnection ?
				new AdoNetSagaPersister(GetOrCreateConnection, SagaTableName, SagaIndexTableName)
				: new AdoNetSagaPersister(ConnectionString, ProviderName, SagaTableName, SagaIndexTableName);
			if (createTables) result.EnsureTablesAreCreated();
			return result;
		}

		#region Basic Saga Persister tests

		[Test]
		public void InsertDoesPersistSagaData()
		{
			var persister = CreatePersister(createTables: true);
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { SomeValueWeCanRecognize = "hello" };

			persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });

			var count = ExecuteScalar(string.Format("SELECT COUNT(*) FROM {0}", Dialect.QuoteForTableName(SagaTableName)));

			Assert.That(count, Is.EqualTo(1));
		}

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

		#region Advanced Saga Persister tests

		[Test]
		public void EnsuresUniquenessAlsoOnCorrelationPropertyWithNull()
		{
			var persister = CreatePersister(createTables: true);
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { SomeValueWeCanRecognize = "hello" };
			var anotherPieceOfDataWithIndexedNullProperty = new SomePieceOfSagaData { SomeValueWeCanRecognize = "hello" };

			persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });

			Assert.Throws<OptimisticLockingException>(() => persister.Insert(anotherPieceOfDataWithIndexedNullProperty, new[] { propertyName }));
		}

		[Test]
		public void CanFindAndUpdateSagaDataByCorrelationPropertyWithNull()
		{
			var persister = CreatePersister(createTables: true);
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { SomeValueWeCanRecognize = "hello" };

			persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });
			var sagaDataFoundViaNullProperty = persister.Find<SomePieceOfSagaData>(propertyName, null);
			Assert.That(sagaDataFoundViaNullProperty, Is.Not.Null, "Could not find saga data with (null) on the correlation property {0}", propertyName);
			Assert.That(sagaDataFoundViaNullProperty.SomeValueWeCanRecognize, Is.EqualTo("hello"));

			sagaDataFoundViaNullProperty.SomeValueWeCanRecognize = "hwello there!!1";
			persister.Update(sagaDataFoundViaNullProperty, new[] { propertyName });
			var sagaDataFoundAgainViaNullProperty = persister.Find<SomePieceOfSagaData>(propertyName, null);
			Assert.That(sagaDataFoundAgainViaNullProperty, Is.Not.Null, "Could not find saga data with (null) on the correlation property {0} after having updated it", propertyName);
			Assert.That(sagaDataFoundAgainViaNullProperty.SomeValueWeCanRecognize, Is.EqualTo("hwello there!!1"));
		}


		#endregion
	}
}