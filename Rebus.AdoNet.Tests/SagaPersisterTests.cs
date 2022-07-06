﻿using System;
using System.IO;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;

using Common.Logging;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using Rhino.Mocks;

using Rebus;
using Rebus.Bus;
using Rebus.Testing;
using Rebus.Shared;

namespace Rebus.AdoNet
{
	[TestFixtureSource(typeof(SagaPersisterTests), nameof(GetConnectionSources))]
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


		class SomeCollectedThing
		{
			public int No { get; set; }
		}

		class SomeEmbeddedThingie
		{
			public SomeEmbeddedThingie()
			{
				Thingies = new List<SomeCollectedThing>();
			}

			public string ThisIsEmbedded { get; set; }
			public List<SomeCollectedThing> Thingies { get; set; }
		}

		class MySagaData : ISagaData
		{
			public string SomeField { get; set; }
			public string AnotherField { get; set; }
			public SomeEmbeddedThingie Embedded { get; set; }
			public Guid Id { get; set; }

			public int Revision { get; set; }
		}

		class GenericSagaData<T> : ISagaData
		{
			public T Property { get; set; }
			public Guid Id { get; set; }
			public int Revision { get; set; }
		}

		class SimpleSagaData : ISagaData
		{
			public string SomeString { get; set; }
			public Guid Id { get; set; }
			public int Revision { get; set; }
		}

		class IEnumerableSagaData : ISagaData
		{
			public string SomeField { get; set; }
			public IEnumerable<string> AnotherFields { get; set; }
			public Guid Id { get; set; }
			public int Revision { get; set; }
		}

		[Flags]
		public enum Feature
		{
			None = 0,
			Arrays = (1<<0),
			Json = (1<<1),
			Locking = (1<<2),
			NoWait = (1<<3)
		}
		#endregion

		private static readonly ILog _Log = LogManager.GetLogger<SagaPersisterTests>();

		private AdoNetConnectionFactory _factory;
		private AdoNetUnitOfWorkManager _manager;
		private readonly Feature _features; 
		private const string SagaTableName = "Sagas";
		private const string SagaIndexTableName = "SagasIndex";

		public SagaPersisterTests(string provider, string connectionString, Feature features)
			: base(provider, connectionString)
		{
			_features = features;
		}

		#region Message Context Helpers

		protected IDisposable EstablishMessageContext()
		{
			var headers = new Dictionary<string, object>
				{
					{Headers.ReturnAddress, "none"},
					{Headers.MessageId, "just_some_message_id"},
				};

			var result = new NoTransaction();
			//_messageContext = _MessageContextEstablishAccessor(headers);
			Disposables.TrackDisposable(EnterAFakeMessageContext(headers: headers));

			return result;
		}

		protected IDisposable EnterAFakeMessageContext(IDictionary<string, object> headers = null, IDictionary<string, object> items = null)
		{
			var current = MessageContext.HasCurrent ? MessageContext.GetCurrent() : null;
			var fakeConcurrentMessageContext = MockRepository.GenerateMock<IMessageContext>();
			fakeConcurrentMessageContext.Stub(x => x.Headers).Return(headers ?? new Dictionary<string, object>());
			fakeConcurrentMessageContext.Stub(x => x.Items).Return(items ?? new Dictionary<string, object>());
			fakeConcurrentMessageContext.Disposed += () =>
			{
				if (current != null) FakeMessageContext.Establish(current);
			};
			return FakeMessageContext.Establish(fakeConcurrentMessageContext);
		}

		#endregion

		public static IEnumerable<object[]> GetConnectionSources()
		{
			var sources = ConnectionSources()
				.Select(x => new
				{
					Provider = x[0],
					ConnectionString = x[1],
					Features = NPGSQL_PROVIDER_NAME == (string)x[0] ? (Feature.Arrays | Feature.Json | Feature.Locking | Feature.NoWait) : Feature.None
				});

			var @base = sources.Select(x => new[] { x.Provider, x.ConnectionString, Feature.None });
			var extra1 = sources.Where(x => x.Features.HasFlag(Feature.Locking)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Locking) });
			var extra2 = sources.Where(x => x.Features.HasFlag(Feature.Locking|Feature.NoWait)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Locking|Feature.NoWait) });
			var extra3 = sources.Where(x => x.Features.HasFlag(Feature.Arrays)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Arrays) });
			var extra4 = sources.Where(x => x.Features.HasFlag(Feature.Arrays|Feature.Locking)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Arrays|Feature.Locking) });
			var extra5 = sources.Where(x => x.Features.HasFlag(Feature.Arrays|Feature.Locking|Feature.NoWait)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Arrays|Feature.Locking|Feature.NoWait) });
			var extra6 = sources.Where(x => x.Features.HasFlag(Feature.Json)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Json) });
			var extra7 = sources.Where(x => x.Features.HasFlag(Feature.Json|Feature.Locking)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Json|Feature.Locking) });
			var extra8 = sources.Where(x => x.Features.HasFlag(Feature.Json|Feature.Locking|Feature.NoWait)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Json|Feature.Locking|Feature.NoWait) });

			return @base.Union(extra1).Union(extra2).Union(extra3).Union(extra4)
						.Union(extra5).Union(extra6).Union(extra7).Union(extra8);
		}

		protected AdoNetSagaPersister CreatePersister(bool basic = false)
		{
			var result = _features.HasFlag(Feature.Json)
				? new AdoNetSagaPersisterAdvanced(_manager, SagaTableName) as AdoNetSagaPersister
				: new AdoNetSagaPersisterLegacy(_manager, SagaTableName, SagaIndexTableName);
			
			if (!basic)
			{
				if (_features.HasFlag(Feature.Arrays)) result.UseSqlArraysForCorrelationIndexes();
				if (_features.HasFlag(Feature.Locking)) result.UseLockingOnSagaUpdates(!_features.HasFlag(Feature.NoWait));
			}
			return result;
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
		
		[OneTimeSetUp]
		public void OneTimeSetup()
		{
			_factory = new AdoNetConnectionFactory(ConnectionString, ProviderName);
			_manager = new AdoNetUnitOfWorkManager(_factory, (fact, cont) => new AdoNetUnitOfWork(fact, cont));

			DropSagaTables();

			if (ProviderName == NPGSQL_PROVIDER_NAME)
			{
				// On PostgreSql we use GIN on some tests..
				ExecuteCommand("CREATE EXTENSION IF NOT EXISTS btree_gin;");
			}

			var persister = CreatePersister();
			persister.EnsureTablesAreCreated();
		}


		[SetUp]
		public new void SetUp()
		{
			if (!_features.HasFlag(Feature.Json)) ExecuteCommand($"DELETE FROM \"{SagaIndexTableName}\";");
			ExecuteCommand($"DELETE FROM \"{SagaTableName}\";");
			
			Disposables.TrackDisposable(EstablishMessageContext()); //< Initial (fake) message under each test will run.
		}

		#region Basic Saga Persister tests

		[Test]
		public void InsertDoesPersistSagaData()
		{
			var persister = CreatePersister();
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { SomeValueWeCanRecognize = "hello" };

			persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });

			var count = ExecuteScalar(string.Format("SELECT COUNT(*) FROM {0}", Dialect.QuoteForTableName(SagaTableName)));

			Assert.That(count, Is.EqualTo(1));
		}

		[Test]
		public void WhenIgnoringNullProperties_DoesNotSaveNullPropertiesOnUpdate()
		{
			var persister = CreatePersister();

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
			var persister = CreatePersister();

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
			var persister = CreatePersister();
				
			// act
			persister.EnsureTablesAreCreated();

			// assert
			var existingTables = GetTableNames();
			Assert.That(existingTables, Contains.Item(SagaTableName));
			
			if (persister is AdoNetSagaPersisterLegacy)
				Assert.That(existingTables, Contains.Item(SagaIndexTableName));
			
			// FIXME: Additional asserts depending on each case
			//  - Ensure index columns are text/array/json..
			//  - Ensure right indexes were created..
		}

		[Test]
		public void DoesntDoAnythingIfTheTablesAreAlreadyThere()
		{
			// arrange
			//DropSagaTables();
			var persister = CreatePersister();
			//ExecuteCommand(@"CREATE TABLE """ + SagaTableName + @""" (""id"" INT NOT NULL)");
			//ExecuteCommand(@"CREATE TABLE """ + SagaIndexTableName + @""" (""id"" INT NOT NULL)");

			// act

			// assert
			persister.EnsureTablesAreCreated();
			persister.EnsureTablesAreCreated();
			persister.EnsureTablesAreCreated();
		}

		// FIXME: Add more tests with different kind of values (Dates, Decimal, Float, etc.), ensuring persisted entries can be found later on.
		
		#endregion

		#region Advanced Saga Persister tests

		private static void TestFindSagaByPropertyWithType<TProperty>(AdoNetSagaPersister persister, TProperty propertyValueToUse)
		{
			var propertyTypeToTest = typeof(TProperty);
			var type = typeof(GenericSagaData<>);
			var sagaDataType = type.MakeGenericType(propertyTypeToTest);
			var savedSagaData = (ISagaData)Activator.CreateInstance(sagaDataType);
			var savedSagaDataId = Guid.NewGuid();
			var propertyName = nameof(GenericSagaData<TProperty>.Property);
			savedSagaData.Id = savedSagaDataId;
			sagaDataType.GetProperty(propertyName).SetValue(savedSagaData, propertyValueToUse, new object[0]);
			persister.Insert(savedSagaData, new[] { propertyName });

			var foundSagaData = persister.Find<GenericSagaData<TProperty>>(propertyName, propertyValueToUse);

			Assert.That(foundSagaData?.Id, Is.EqualTo(savedSagaDataId));
		}

		private MySagaData SagaData(int someNumber, string textInSomeField)
		{
			return new MySagaData
			{
				Id = Guid.NewGuid(),
				SomeField = someNumber.ToString(),
				AnotherField = textInSomeField,
			};
		}

		private IEnumerableSagaData EnumerableSagaData(int someNumber, IEnumerable<string> multipleTexts)
		{
			return new IEnumerableSagaData
			{
				Id = Guid.NewGuid(),
				SomeField = someNumber.ToString(),
				AnotherFields = multipleTexts
			};
		}

		[Test]
		public void CanFindAndUpdateSagaDataByCorrelationPropertyWithNull()
		{
			var persister = CreatePersister();
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { Id = Guid.NewGuid(), SomeValueWeCanRecognize = "hello" };

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

		[Test]
		public void PersisterCanFindSagaByPropertiesWithDifferentDataTypes()
		{
			var persister = CreatePersister();
			TestFindSagaByPropertyWithType(persister, "Hello worlds!!");
			TestFindSagaByPropertyWithType(persister, 23);
			TestFindSagaByPropertyWithType(persister, Guid.NewGuid());
		}

		[Test]
		public void PersisterCanFindSagaById()
		{
			var persister = CreatePersister();
			var savedSagaData = new MySagaData();
			var savedSagaDataId = Guid.NewGuid();
			savedSagaData.Id = savedSagaDataId;
			persister.Insert(savedSagaData, new string[0]);

			var foundSagaData = persister.Find<MySagaData>("Id", savedSagaDataId);

			Assert.That(foundSagaData.Id, Is.EqualTo(savedSagaDataId));
		}

		[Test]
		public void PersistsComplexSagaLikeExpected()
		{
			var persister = CreatePersister();
			var sagaDataId = Guid.NewGuid();

			var complexPieceOfSagaData =
				new MySagaData
				{
					Id = sagaDataId,
					SomeField = "hello",
					AnotherField = "world!",
					Embedded = new SomeEmbeddedThingie
					{
						ThisIsEmbedded = "this is embedded",
						Thingies =
							{
								new SomeCollectedThing { No = 1 },
								new SomeCollectedThing { No = 2 },
								new SomeCollectedThing { No = 3 },
								new SomeCollectedThing { No = 4 }
							}
					}
				};

			persister.Insert(complexPieceOfSagaData, new[] { "SomeField" });

			var sagaData = persister.Find<MySagaData>("Id", sagaDataId);
			Assert.That(sagaData.SomeField, Is.EqualTo("hello"));
			Assert.That(sagaData.AnotherField, Is.EqualTo("world!"));
		}

		[Test]
		public void CanDeleteSaga()
		{
			const string someStringValue = "whoolala";

			var persister = CreatePersister();
			var mySagaDataId = Guid.NewGuid();
			var mySagaData = new SimpleSagaData
			{
				Id = mySagaDataId,
				SomeString = someStringValue
			};

			persister.Insert(mySagaData, new[] { "SomeString" });
			var sagaDataToDelete = persister.Find<SimpleSagaData>("Id", mySagaDataId);

			persister.Delete(sagaDataToDelete);

			var sagaData = persister.Find<SimpleSagaData>("Id", mySagaDataId);
			Assert.That(sagaData, Is.Null);
		}

		[Test]
		public void CanFindSagaByPropertyValues()
		{
			var persister = CreatePersister();

			persister.Insert(SagaData(1, "some field 1"), new[] { "AnotherField" });
			persister.Insert(SagaData(2, "some field 2"), new[] { "AnotherField" });
			persister.Insert(SagaData(3, "some field 3"), new[] { "AnotherField" });

			var dataViaNonexistentValue = persister.Find<MySagaData>("AnotherField", "non-existent value");
			var dataViaNonexistentField = persister.Find<MySagaData>("SomeFieldThatDoesNotExist", "doesn't matter");
			var mySagaData = persister.Find<MySagaData>("AnotherField", "some field 2");

			Assert.That(dataViaNonexistentField, Is.Null);
			Assert.That(dataViaNonexistentValue, Is.Null);
			Assert.That(mySagaData, Is.Not.Null);
			Assert.That(mySagaData?.SomeField, Is.EqualTo("2"));
		}

		[Test]
		public void CanFindSagaWithIEnumerableAsCorrelatorId()
		{
			var persister = CreatePersister();

			persister.Insert(EnumerableSagaData(3, new string[] { "Field 1", "Field 2", "Field 3"}), new[] { "AnotherFields" });

			var dataViaNonexistentValue = persister.Find<IEnumerableSagaData>("AnotherFields", "non-existent value");
			var dataViaNonexistentField = persister.Find<IEnumerableSagaData>("SomeFieldThatDoesNotExist", "doesn't matter");
			var mySagaData = persister.Find<IEnumerableSagaData>("AnotherFields", "Field 3");
			
			Assert.That(dataViaNonexistentField, Is.Null);
			Assert.That(dataViaNonexistentValue, Is.Null);
			Assert.That(mySagaData, Is.Not.Null);
			Assert.That(mySagaData?.SomeField, Is.EqualTo("3"));
		}

		[Test]
		public void SamePersisterCanSaveMultipleTypesOfSagaDatas()
		{
			var persister = CreatePersister();
			var sagaId1 = Guid.NewGuid();
			var sagaId2 = Guid.NewGuid();
			persister.Insert(new SimpleSagaData { Id = sagaId1, SomeString = "Ol�" }, new[] { "Id" });
			persister.Insert(new MySagaData { Id = sagaId2, AnotherField = "Yipiie" }, new[] { "Id" });

			var saga1 = persister.Find<SimpleSagaData>("Id", sagaId1);
			var saga2 = persister.Find<MySagaData>("Id", sagaId2);

			Assert.That(saga1.SomeString, Is.EqualTo("Ol�"));
			Assert.That(saga2.AnotherField, Is.EqualTo("Yipiie"));
		}


		[Test]
		public void PersisterCanFindSagaDataWithNestedElements()
		{
			const string stringValue = "I expect to find something with this string!";

			var persister = CreatePersister();
			var path = Reflect.Path<SagaDataWithNestedElement>(d => d.ThisOneIsNested.SomeString);

			persister.Insert(new SagaDataWithNestedElement
			{
				Id = Guid.NewGuid(),
				Revision = 12,
				ThisOneIsNested = new ThisOneIsNested
				{
					SomeString = stringValue
				}
			}, new[] { path });

			var loadedSagaData = persister.Find<SagaDataWithNestedElement>(path, stringValue);

			Assert.That(loadedSagaData?.ThisOneIsNested, Is.Not.Null);
			Assert.That(loadedSagaData?.ThisOneIsNested.SomeString, Is.EqualTo(stringValue));
		}

		#endregion

		#region Uniqueness Of CorrelationIds

		internal class SomeSaga : ISagaData
		{
			public Guid Id { get; set; }
			public int Revision { get; set; }

			public string SomeCorrelationId { get; set; }
		}

		[Test]
		public void CanUpdateSaga()
		{
			// arrange
			const string theValue = "this is just some value";

			var persister = CreatePersister();
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };

			var propertyPath = Reflect.Path<SomeSaga>(s => s.SomeCorrelationId);
			var pathsToIndex = new[] { propertyPath };
			persister.Insert(firstSaga, pathsToIndex);

			var sagaToUpdate = persister.Find<SomeSaga>(propertyPath, theValue);

			Assert.DoesNotThrow(() => persister.Update(sagaToUpdate, pathsToIndex));
		}

		[Test, Description("We don't allow two sagas to have the same value of a property that is used to correlate with incoming messages, " +
						   "because that would cause an ambiguity if an incoming message suddenly mathed two or more sagas... " +
						   "moreover, e.g. MongoDB would not be able to handle the message and update multiple sagas reliably because it doesn't have transactions.")]
		public void CannotInsertAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange
			var persister = CreatePersister();
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };

			if (persister is ICanUpdateMultipleSagaDatasAtomically)
			{
				Assert.Ignore("Ignore test as persister does actually support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };
			persister.Insert(firstSaga, pathsToIndex);

			// act
			// assert
			Assert.Throws<OptimisticLockingException>(() => persister.Insert(secondSaga, pathsToIndex));
		}

		[Test]
		public void CannotUpdateAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange  
			var persister = CreatePersister();
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = "other value" };

			if (persister is ICanUpdateMultipleSagaDatasAtomically)
			{
				Assert.Ignore("Ignore test as persister does actually support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };
			persister.Insert(firstSaga, pathsToIndex);
			persister.Insert(secondSaga, pathsToIndex);

			// act
			// assert
			secondSaga.SomeCorrelationId = theValue;
			Assert.Throws<OptimisticLockingException>(() => persister.Update(secondSaga, pathsToIndex));
		}

		[Test]
		[Description("This is the opposite of CannotInsertAnotherSagaWithDuplicateCorrelationId, for persistes supporting atomic saga updates.")]
		public void CanInsertAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange
			var persister = CreatePersister();
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };

			if (!(persister is ICanUpdateMultipleSagaDatasAtomically))
			{
				Assert.Ignore("Ignore test as persister does not support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };

			// act
			persister.Insert(firstSaga, pathsToIndex);
			persister.Insert(secondSaga, pathsToIndex);

			// assert
		}

		[Test]
		public void CanUpdateAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange  
			var persister = CreatePersister();
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = "other value" };

			if (!(persister is ICanUpdateMultipleSagaDatasAtomically))
			{
				Assert.Ignore("Ignore test as persister does not support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };

			// act
			persister.Insert(firstSaga, pathsToIndex);
			persister.Insert(secondSaga, pathsToIndex);

			secondSaga.SomeCorrelationId = theValue;
			persister.Update(secondSaga, pathsToIndex);

			// assert
		}

		[Test]
		public void EnsuresUniquenessAlsoOnCorrelationPropertyWithNull()
		{
			var persister = CreatePersister();
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { Id = Guid.NewGuid(), SomeValueWeCanRecognize = "hello" };
			var anotherPieceOfDataWithIndexedNullProperty = new SomePieceOfSagaData { Id = Guid.NewGuid(), SomeValueWeCanRecognize = "hello" };

			persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });

			Assert.That(
				() => persister.Insert(anotherPieceOfDataWithIndexedNullProperty, new[] { propertyName }),
				(persister is ICanUpdateMultipleSagaDatasAtomically) ? (IResolveConstraint)Throws.Nothing : Throws.Exception
			);
		}

		#endregion

		#region Optimistic Concurrency

		[Test]
		public void UsesOptimisticLockingAndDetectsRaceConditionsWhenUpdatingFindingBySomeProperty()
		{
			var persister = CreatePersister();
			var indexBySomeString = new[] { "SomeString" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id, SomeString = "hello world!" };
			persister.Insert(simpleSagaData, indexBySomeString);

			var sagaData1 = persister.Find<SimpleSagaData>("SomeString", "hello world!");
			
			Assert.That(sagaData1, Is.Not.Null);
			
			sagaData1.SomeString = "I changed this on one worker";

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = persister.Find<SimpleSagaData>("SomeString", "hello world!");
				sagaData2.SomeString = "I changed this on another worker";
				persister.Update(sagaData2, indexBySomeString);
			}

			Assert.Throws<OptimisticLockingException>(() => persister.Insert(sagaData1, indexBySomeString));
		}

		[Test]
		public void UsesOptimisticLockingAndDetectsRaceConditionsWhenUpdatingFindingById()
		{
			var persister = CreatePersister();
			var indexBySomeString = new[] { "Id" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id, SomeString = "hello world!" };
			persister.Insert(simpleSagaData, indexBySomeString);

			var sagaData1 = persister.Find<SimpleSagaData>("Id", id);
			sagaData1.SomeString = "I changed this on one worker";

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = persister.Find<SimpleSagaData>("Id", id);
				sagaData2.SomeString = "I changed this on another worker";
				persister.Update(sagaData2, indexBySomeString);
			}

			Assert.Throws<OptimisticLockingException>(() => persister.Insert(sagaData1, indexBySomeString));
		}

		[Test]
		public void ConcurrentDeleteAndUpdateThrowsOnUpdate()
		{
			var persister = CreatePersister();
			var indexBySomeString = new[] { "Id" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id };

			persister.Insert(simpleSagaData, indexBySomeString);
			var sagaData1 = persister.Find<SimpleSagaData>("Id", id);
			sagaData1.SomeString = "Some new value";

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = persister.Find<SimpleSagaData>("Id", id);
				persister.Delete(sagaData2);
			}

			Assert.Throws<OptimisticLockingException>(() => persister.Update(sagaData1, indexBySomeString));
		}

		[Test]
		public void ConcurrentDeleteAndUpdateThrowsOnDelete()
		{
			var persister = CreatePersister();
			var indexBySomeString = new[] { "Id" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id };

			persister.Insert(simpleSagaData, indexBySomeString);
			var sagaData1 = persister.Find<SimpleSagaData>("Id", id);

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = persister.Find<SimpleSagaData>("Id", id);
				sagaData2.SomeString = "Some new value";
				persister.Update(sagaData2, indexBySomeString);
			}

			Assert.Throws<OptimisticLockingException>(() => persister.Delete(sagaData1));
		}

		[Test]
		public void InsertingTheSameSagaDataTwiceGeneratesAnError()
		{
			// arrange
			var persister = CreatePersister();
			var sagaDataPropertyPathsToIndex = new[] { Reflect.Path<SimpleSagaData>(d => d.Id) };

			var sagaId = Guid.NewGuid();
			persister.Insert(new SimpleSagaData { Id = sagaId, Revision = 0, SomeString = "hello!" },
							 sagaDataPropertyPathsToIndex);

			// act
			// assert
			Assert.Throws<OptimisticLockingException>(
				() => persister.Insert(new SimpleSagaData { Id = sagaId, Revision = 0, SomeString = "hello!" },
									   sagaDataPropertyPathsToIndex));
		}

		#endregion

		#region Update Multiple Sagas Atomically

		[Test]
		public void CanInsertTwoSagasUnderASingleUoW()
		{
			var persister = CreatePersister();
			var sagaId1 = Guid.NewGuid();
			var sagaId2 = Guid.NewGuid();

			using (var uow = _manager.Create())
			{
				persister.Insert(new SimpleSagaData { Id = sagaId1, SomeString = "FirstSaga" }, new[] { "Id" });
				persister.Insert(new MySagaData { Id = sagaId2, AnotherField = "SecondSaga" }, new[] { "Id" });

				uow.Commit();
			}

			using (EnterAFakeMessageContext())
			{
				var saga1 = persister.Find<SimpleSagaData>("Id", sagaId1);
				var saga2 = persister.Find<MySagaData>("Id", sagaId2);

				Assert.That(saga1.SomeString, Is.EqualTo("FirstSaga"));
				Assert.That(saga2.AnotherField, Is.EqualTo("SecondSaga"));
			}
		}

		[Test]
		public void NoChangesAreMadeWhenUoWIsNotCommitted()
		{
			var persister = CreatePersister();
			var sagaId1 = Guid.NewGuid();
			var sagaId2 = Guid.NewGuid();

			using (var uow = _manager.Create())
			{
				persister.Insert(new SimpleSagaData { Id = sagaId1, SomeString = "FirstSaga" }, new[] { "Id" });
				persister.Insert(new MySagaData { Id = sagaId2, AnotherField = "SecondSaga" }, new[] { "Id" });

				// XXX: Purposedly not committed.
			}

			using (EnterAFakeMessageContext())
			{
				var saga1 = persister.Find<SimpleSagaData>("Id", sagaId1);
				var saga2 = persister.Find<MySagaData>("Id", sagaId2);

				Assert.That(saga1, Is.Null);
				Assert.That(saga2, Is.Null);
			}
		}

		#endregion

		#region Saga Locking
		[Test]
		public void FindSameSagaTwiceThrowsOnNoWait()
		{
			// Skip this test on sqlite, as it does not support locking.
			if (!_features.HasFlag(Feature.NoWait))
			{
				Assert.Ignore("Test not supported when NoWait Locking is disabled or unsupported.");
				return;
			}

			var persister = CreatePersister();
			var savedSagaData = new MySagaData();
			var savedSagaDataId = Guid.NewGuid();
			savedSagaData.Id = savedSagaDataId;
			persister.Insert(savedSagaData, new string[0]);

			using (_manager.Create())
			using (_manager.GetScope(autocomplete: true))
			{
				persister.Find<MySagaData>("Id", savedSagaDataId);

				Assert.Throws<AdoNetSagaLockedException>(() =>
				{
					//using (var thread = new CrossThreadRunner(() =>
					//{
						using (EnterAFakeMessageContext())
						//using (_manager.Create())
						//using (_manager.GetScope(autocomplete: true))
						{
							//_manager.GetScope().Connection.ExecuteCommand("SET TRANSACTION ISOLATION LEVEL  READ COMMITTED;");
							persister.Find<MySagaData>("Id", savedSagaDataId);
						}
					//}))
					//{
					//	thread.Run();
					//}
				});
			}
		}
		#endregion

	}
}