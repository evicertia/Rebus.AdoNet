using System;
using System.IO;
using System.Data;
using System.Linq;
using System.Collections;
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
			public ThisOneIsNested ThisOneIsNestedToo { get; set; }
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

		private class ComplexSaga : ISagaData
		{
			#region Inner Types

			public enum Values
			{
				Unknown = 0,
				Valid
			}

			[Flags]
			public enum Flags
			{
				Unknown = 1 << 0,
				One = 1 << 1,
				Two = 1 << 2
			}

			#endregion

			#region Properties

			public Guid Id { get; set; }
			public int Revision { get; set; }

			public Guid Uuid { get; set; }
			public Guid? NullableGuid { get; set; }
			public char Char { get; set; }
			public string Text { get; set; }
			public bool Bool { get; set; }
			public sbyte SByte { get; set; }
			public byte Byte { get; set; }
			public ushort UShort { get; set; }
			public short Short { get; set; }
			public uint UInt { get; set; }
			public int Int { get; set; }
			public ulong ULong { get; set; }
			public long Long { get; set; }
			public float Float { get; set; }
			public double Double { get; set; }
			public decimal Decimal { get; set; }
			public DateTime Date { get; set; }
			public TimeSpan Time { get; set; }
			public Values Enum { get; set; }
			public Flags EnumFlags { get; set; }
			public object Object { get; set; }
			public IEnumerable<string> Strings { get; set; }
			public IEnumerable<decimal> Decimals { get; set; }

			#endregion
		}

		[Flags]
		public enum Feature
		{
			None = 0,
			Arrays = (1 << 0),
			Json = (1 << 1),
			Locking = (1 << 2),
			NoWait = (1 << 3)
		}
		#endregion

		private static readonly ILog _Log = LogManager.GetLogger<SagaPersisterTests>();

		private static readonly IDictionary<string, PropertyInfo> _correlations = typeof(ComplexSaga).GetProperties(BindingFlags.Public | BindingFlags.Instance)
			.Where(x => x.Name != nameof(ISagaData.Id) && x.Name != nameof(ISagaData.Revision))
			.ToDictionary(x => x.Name, x => x);

		private AdoNetConnectionFactory _factory;
		private AdoNetUnitOfWorkManager _manager;
		private readonly Feature _features;
		private const string SagaTableName = "Sagas";
		private const string SagaIndexTableName = "SagasIndex";

		private AdoNetSagaPersister _persister;

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
			var extra2 = sources.Where(x => x.Features.HasFlag(Feature.Locking | Feature.NoWait)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Locking | Feature.NoWait) });
			var extra3 = sources.Where(x => x.Features.HasFlag(Feature.Arrays)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Arrays) });
			var extra4 = sources.Where(x => x.Features.HasFlag(Feature.Arrays | Feature.Locking)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Arrays | Feature.Locking) });
			var extra5 = sources.Where(x => x.Features.HasFlag(Feature.Arrays | Feature.Locking | Feature.NoWait)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Arrays | Feature.Locking | Feature.NoWait) });
			var extra6 = sources.Where(x => x.Features.HasFlag(Feature.Json)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Json) });
			var extra7 = sources.Where(x => x.Features.HasFlag(Feature.Json | Feature.Locking)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Json | Feature.Locking) });
			var extra8 = sources.Where(x => x.Features.HasFlag(Feature.Json | Feature.Locking | Feature.NoWait)).Select(x => new[] { x.Provider, x.ConnectionString, (Feature.Json | Feature.Locking | Feature.NoWait) });

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

			_persister = CreatePersister();
			_persister.EnsureTablesAreCreated();
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
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { SomeValueWeCanRecognize = "hello" };

			_persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });

			var count = ExecuteScalar(string.Format("SELECT COUNT(*) FROM {0}", Dialect.QuoteForTableName(SagaTableName)));

			Assert.That(count, Is.EqualTo(1));
		}

		[Test]
		public void WhenIgnoringNullProperties_DoesNotSaveNullPropertiesOnUpdate()
		{
			_persister.DoNotIndexNullProperties();

			const string correlationProperty1 = "correlation property 1";
			const string correlationProperty2 = "correlation property 2";
			var correlationPropertyPaths = new[]
			{
				Reflect.Path<PieceOfSagaData>(s => s.SomeProperty),
				Reflect.Path<PieceOfSagaData>(s => s.AnotherProperty)
			};

			var firstPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData { SomeProperty = correlationProperty1, AnotherProperty = "random12423" };
			var nextPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData { SomeProperty = correlationProperty2, AnotherProperty = "random38791387" };

			_persister.Insert(firstPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);
			_persister.Insert(nextPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			var firstPiece = _persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty1);
			firstPiece.AnotherProperty = null;
			_persister.Update(firstPiece, correlationPropertyPaths);

			var nextPiece = _persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty2);
			nextPiece.AnotherProperty = null;
			_persister.Update(nextPiece, correlationPropertyPaths);
		}

		[Test]
		public void WhenIgnoringNullProperties_DoesNotSaveNullPropertiesOnInsert()
		{
			_persister.DoNotIndexNullProperties();

			const string correlationProperty1 = "correlation property 1";
			const string correlationProperty2 = "correlation property 2";
			var correlationPropertyPaths = new[]
			{
				Reflect.Path<PieceOfSagaData>(s => s.SomeProperty),
				Reflect.Path<PieceOfSagaData>(s => s.AnotherProperty)
			};

			var firstPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData {
				SomeProperty = correlationProperty1
			};

			var nextPieceOfSagaDataWithNullValueOnProperty = new PieceOfSagaData {
				SomeProperty = correlationProperty2
			};

			var firstId = firstPieceOfSagaDataWithNullValueOnProperty.Id;
			var nextId = nextPieceOfSagaDataWithNullValueOnProperty.Id;

			_persister.Insert(firstPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			// must not throw:
			_persister.Insert(nextPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			var firstPiece = _persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty1);
			var nextPiece = _persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty2);

			Assert.That(firstPiece.Id, Is.EqualTo(firstId));
			Assert.That(nextPiece.Id, Is.EqualTo(nextId));
		}

		[Test]
		public void SchemaIsCreatedAsExpected()
		{
			var tableNames = GetTableNames();
			Assert.That(tableNames, Contains.Item(SagaTableName), "Missing main saga table?!");

			var schema = GetColumnSchemaFor(SagaTableName); //< (item1: COLUMN_NAME, item2: DATA_TYPE, [...?])
			var indexes = GetIndexesFor(SagaTableName);

			// NOTE: Some 'hacks' for postgres/yuga.. returned column type for strings are varchar(xxx)..
			//		 remove the length.
			var stringDbType = Dialect.GetColumnType(DbType.String).ToLower();
			if (stringDbType.EndsWith(")"))
				stringDbType = stringDbType.Substring(0, stringDbType.IndexOf("("));

			Assert.That(schema.First(x => x.Item1 == "id").Item2, Is.EqualTo(Dialect.GetColumnType(DbType.Guid).ToLower()), "#0.0");
			Assert.That(schema.First(x => x.Item1 == "revision").Item2, Is.EqualTo(Dialect.GetColumnType(DbType.Int32).ToLower()), "#0.1");
			Assert.That(schema.First(x => x.Item1 == "saga_type").Item2, Is.EqualTo(stringDbType), "#0.2");
			Assert.That(schema.First(x => x.Item1 == "data").Item2, Is.EqualTo("text"), "#0.3");
			Assert.That(indexes, Contains.Item($"ix_{SagaTableName}_id_saga_type"), "#0.4");

			if (_persister is AdoNetSagaPersisterLegacy) //< NOTE: DATA_TYPE for PostgresSQL arrays are _text instead of text[].
			{
				var sagaIndexSchema = GetColumnSchemaFor(SagaIndexTableName);
				var sagaIndexIndexes = GetIndexesFor(SagaIndexTableName);
				var isArray = _features.HasFlag(Feature.Arrays);

				Assert.That(tableNames, Contains.Item(SagaIndexTableName), "Missing saga indexes table?!");
				Assert.That(sagaIndexSchema.First(x => x.Item1 == "saga_id").Item2, Is.EqualTo(Dialect.GetColumnType(DbType.Guid).ToLower()), "#1.0");
				Assert.That(sagaIndexSchema.First(x => x.Item1 == "key").Item2, Is.EqualTo(stringDbType), "#1.1");
				Assert.That(sagaIndexSchema.First(x => x.Item1 == "value").Item2, Is.EqualTo("text"), "#1.2");
				Assert.That(sagaIndexSchema.First(x => x.Item1 == "values").Item2, Is.EqualTo(isArray ? "_text" : "text"), "#1.3");
				Assert.That(sagaIndexIndexes, Contains.Item($"ix_{SagaIndexTableName}_key_value"), "#1.4");
				Assert.That(sagaIndexIndexes, Contains.Item($"ix_{SagaIndexTableName}_key_values"), "#1.5");
			}
			else if (_persister is AdoNetSagaPersisterAdvanced)
			{
				var correlationsType = schema.First(x => x.Item1 == "correlations").Item2;
				Assert.That(tableNames, Does.Not.Contains(SagaIndexTableName), "Have we created saga indexes table?!");
				Assert.That(correlationsType, Is.EqualTo("jsonb"), "#2.0");

				var ix = Dialect.SupportsMultiColumnGinIndexes ? $"ix_{SagaTableName}_saga_type_correlations" : $"ix_{SagaTableName}_correlations";
				Assert.That(indexes, Contains.Item(ix), "#2.1");
			}
			else //< If someday we add another persister.. we should add coverage for it.
			{
				throw new NotSupportedException("Missing coverage for a new persister?!?!");
			}
		}

		[Test]
		public void DoesntDoAnythingIfTheTablesAreAlreadyThere()
		{
			Assert.That(() =>
			{
				_persister.EnsureTablesAreCreated();
				_persister.EnsureTablesAreCreated();
				_persister.EnsureTablesAreCreated();
			}, Throws.Nothing);
		}

		[Test]
		public void SagaDataCanBeRecoveredWithDifferentKindOfValuesAsCorrelations()
		{
			var data = new ComplexSaga()
			{
				Id = Guid.NewGuid(),
				Uuid = Guid.NewGuid(),
				NullableGuid = Guid.NewGuid(),
				Char = '\n',
				Text = "this is a string w/ spanish characters like EÑE.",
				Bool = true,
				SByte = sbyte.MinValue,
				Byte = byte.MaxValue,
				UShort = ushort.MinValue,
				Short = short.MaxValue,
				UInt = uint.MinValue,
				Int = int.MaxValue,
				ULong = ulong.MinValue,
				Long = long.MaxValue,
				Float = float.NaN,
				Double = double.PositiveInfinity,
				Decimal = 10000.746525344148M,
				Date = DateTime.UtcNow,
				Time = TimeSpan.MaxValue,
				Enum = ComplexSaga.Values.Valid,
				EnumFlags = ComplexSaga.Flags.One | ComplexSaga.Flags.Two,
				Object = 2.7878798745M,
				Strings = new[] { "x", "y" },
				Decimals = new[] { 0.646584564M, 6.98984564544212M }
			};

			_persister.Insert(data, _correlations.Keys.ToArray());

			Assert.Multiple(() =>
			{
				foreach (var correlation in _correlations)
				{
					var value = correlation.Value.GetValue(data);
					var recovered = _persister.Find<ComplexSaga>(correlation.Key, value);
					Assert.That(recovered, Is.Not.Null, "Can't recover saga using correlation: {0}?!", correlation.Key);

					// XXX: If the property is a collection like an array.. we'll do an extra check (looking for the saga using an inner value).
					//		Using a KeyValuePair<,> as correlation is a corner case that we don't want to support.
					if (value is IEnumerable enumerable && !(value is string) && !(value is IDictionary))
					{
						foreach (var property in enumerable)
						{
							recovered = _persister.Find<ComplexSaga>(correlation.Key, property);
							Assert.That(recovered, Is.Not.Null, "Can't recover saga using correlation: {0} - {1}?!", correlation.Key, property);
						}
					}

					recovered = _persister.Find<ComplexSaga>(correlation.Key, "None has this correlation");
					Assert.That(recovered, Is.Null, "Something went wrong using correlation: {0}?!", correlation.Key);
				}
			});
		}

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
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { Id = Guid.NewGuid(), SomeValueWeCanRecognize = "hello" };

			_persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });
			var sagaDataFoundViaNullProperty = _persister.Find<SomePieceOfSagaData>(propertyName, null);
			Assert.That(sagaDataFoundViaNullProperty, Is.Not.Null, "Could not find saga data with (null) on the correlation property {0}", propertyName);
			Assert.That(sagaDataFoundViaNullProperty.SomeValueWeCanRecognize, Is.EqualTo("hello"));

			sagaDataFoundViaNullProperty.SomeValueWeCanRecognize = "hwello there!!1";
			_persister.Update(sagaDataFoundViaNullProperty, new[] { propertyName });
			var sagaDataFoundAgainViaNullProperty = _persister.Find<SomePieceOfSagaData>(propertyName, null);
			Assert.That(sagaDataFoundAgainViaNullProperty, Is.Not.Null, "Could not find saga data with (null) on the correlation property {0} after having updated it", propertyName);
			Assert.That(sagaDataFoundAgainViaNullProperty.SomeValueWeCanRecognize, Is.EqualTo("hwello there!!1"));
		}

		[Test]
		public void PersisterCanFindSagaByPropertiesWithDifferentDataTypes()
		{
			TestFindSagaByPropertyWithType(_persister, "Hello worlds!!");
			TestFindSagaByPropertyWithType(_persister, 23);
			TestFindSagaByPropertyWithType(_persister, Guid.NewGuid());
		}

		[Test]
		public void PersisterCanFindSagaById()
		{
			var savedSagaData = new MySagaData();
			var savedSagaDataId = Guid.NewGuid();
			savedSagaData.Id = savedSagaDataId;
			_persister.Insert(savedSagaData, new string[0]);

			var foundSagaData = _persister.Find<MySagaData>("Id", savedSagaDataId);

			Assert.That(foundSagaData.Id, Is.EqualTo(savedSagaDataId));
		}

		[Test]
		public void PersistsComplexSagaLikeExpected()
		{
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

			_persister.Insert(complexPieceOfSagaData, new[] { "SomeField" });

			var sagaData = _persister.Find<MySagaData>("Id", sagaDataId);
			Assert.That(sagaData.SomeField, Is.EqualTo("hello"));
			Assert.That(sagaData.AnotherField, Is.EqualTo("world!"));
		}

		[Test]
		public void CanDeleteSaga()
		{
			const string someStringValue = "whoolala";

			var mySagaDataId = Guid.NewGuid();
			var mySagaData = new SimpleSagaData
			{
				Id = mySagaDataId,
				SomeString = someStringValue
			};

			_persister.Insert(mySagaData, new[] { "SomeString" });
			var sagaDataToDelete = _persister.Find<SimpleSagaData>("Id", mySagaDataId);

			_persister.Delete(sagaDataToDelete);

			var sagaData = _persister.Find<SimpleSagaData>("Id", mySagaDataId);
			Assert.That(sagaData, Is.Null);
		}

		[Test]
		public void CanFindSagaByPropertyValues()
		{
			_persister.Insert(SagaData(1, "some field 1"), new[] { "AnotherField" });
			_persister.Insert(SagaData(2, "some field 2"), new[] { "AnotherField" });
			_persister.Insert(SagaData(3, "some field 3"), new[] { "AnotherField" });

			var dataViaNonexistentValue = _persister.Find<MySagaData>("AnotherField", "non-existent value");
			var dataViaNonexistentField = _persister.Find<MySagaData>("SomeFieldThatDoesNotExist", "doesn't matter");
			var mySagaData = _persister.Find<MySagaData>("AnotherField", "some field 2");

			Assert.That(dataViaNonexistentField, Is.Null);
			Assert.That(dataViaNonexistentValue, Is.Null);
			Assert.That(mySagaData, Is.Not.Null);
			Assert.That(mySagaData?.SomeField, Is.EqualTo("2"));
		}

		[Test]
		public void CanFindSagaWithIEnumerableAsCorrelatorId()
		{

			_persister.Insert(EnumerableSagaData(3, new string[] { "Field 1", "Field 2", "Field 3" }), new[] { "AnotherFields" });

			var dataViaNonexistentValue = _persister.Find<IEnumerableSagaData>("AnotherFields", "non-existent value");
			var dataViaNonexistentField = _persister.Find<IEnumerableSagaData>("SomeFieldThatDoesNotExist", "doesn't matter");
			var mySagaData = _persister.Find<IEnumerableSagaData>("AnotherFields", "Field 3");

			Assert.That(dataViaNonexistentField, Is.Null);
			Assert.That(dataViaNonexistentValue, Is.Null);
			Assert.That(mySagaData, Is.Not.Null);
			Assert.That(mySagaData?.SomeField, Is.EqualTo("3"));
		}

		[Test]
		public void SamePersisterCanSaveMultipleTypesOfSagaDatas()
		{
			var sagaId1 = Guid.NewGuid();
			var sagaId2 = Guid.NewGuid();
			_persister.Insert(new SimpleSagaData { Id = sagaId1, SomeString = "Ol�" }, new[] { "Id" });
			_persister.Insert(new MySagaData { Id = sagaId2, AnotherField = "Yipiie" }, new[] { "Id" });

			var saga1 = _persister.Find<SimpleSagaData>("Id", sagaId1);
			var saga2 = _persister.Find<MySagaData>("Id", sagaId2);

			Assert.That(saga1.SomeString, Is.EqualTo("Ol�"));
			Assert.That(saga2.AnotherField, Is.EqualTo("Yipiie"));
		}


		[Test]
		public void PersisterCanFindSagaDataWithNestedElements()
		{
			var one = new SagaDataWithNestedElement()
			{
				Id = Guid.NewGuid(),
				Revision = 1,
				ThisOneIsNested = new ThisOneIsNested() { SomeString = "1" },
				ThisOneIsNestedToo = new ThisOneIsNested() { SomeString = "999" }
			};

			var two = new SagaDataWithNestedElement()
			{
				Id = Guid.NewGuid(),
				Revision = 2,
				ThisOneIsNested = new ThisOneIsNested() { SomeString = "999" },
				ThisOneIsNestedToo = new ThisOneIsNested() { SomeString = "2" }
			};

			var notFound = new SagaDataWithNestedElement()
			{
				Id = Guid.NewGuid(),
				Revision = 404,
				ThisOneIsNested = new ThisOneIsNested() { SomeString = "2" },
				ThisOneIsNestedToo = new ThisOneIsNested() { SomeString = "1" }
			};

			var pathOne = Reflect.Path<SagaDataWithNestedElement>(x => x.ThisOneIsNested.SomeString);
			var pathTwo = Reflect.Path<SagaDataWithNestedElement>(x => x.ThisOneIsNestedToo.SomeString);

			_persister.Insert(one, new[] { pathOne });
			_persister.Insert(two, new[] { pathTwo });
			_persister.Insert(notFound, new[] { pathOne, pathTwo });

			var recoveredOne = _persister.Find<SagaDataWithNestedElement>(pathOne, "1");
			var recoveredTwo = _persister.Find<SagaDataWithNestedElement>(pathTwo, "2");
			Assert.That(recoveredOne?.Id, Is.EqualTo(one.Id), "Expected to recovered one.");
			Assert.That(recoveredTwo?.Id, Is.EqualTo(two.Id), "Expected to recovered one.");
			Assert.That(_persister.Find<SagaDataWithNestedElement>(pathOne, "3"), Is.Null);
			Assert.That(_persister.Find<SagaDataWithNestedElement>(pathTwo, "3"), Is.Null);
		}

		[Test]
		public void PersisterDiscardCorrelationsThatShouldNotBeIndexed()
		{
			var data = new ComplexSaga() {
				Id = Guid.NewGuid(),
				Uuid = Guid.Empty,
				NullableGuid = Guid.Empty,
				Date = default(DateTime),
				Text = null
			};

			var correlations = new Dictionary<string, object>() {
				[nameof(ComplexSaga.Uuid)] = Guid.Empty,
				[nameof(ComplexSaga.NullableGuid)] = Guid.Empty,
				[nameof(ComplexSaga.Date)] = default(DateTime),
				[nameof(ComplexSaga.Text)] = (string)null
			};

			_persister.DoNotIndexNullProperties();
			_persister.Insert(data, correlations.Keys.ToArray());

			Assert.Multiple(() => {
				foreach (var correlation in correlations) {
					var recovered = _persister.Find<ComplexSaga>(correlation.Key, correlation.Value);
					Assert.That(recovered, Is.Null, "Saga should not be recovered w/ correlation {0} as should be discarded bc null/def.", correlation.Key);
				}
			});
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

			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };

			var propertyPath = Reflect.Path<SomeSaga>(s => s.SomeCorrelationId);
			var pathsToIndex = new[] { propertyPath };
			_persister.Insert(firstSaga, pathsToIndex);

			var sagaToUpdate = _persister.Find<SomeSaga>(propertyPath, theValue);

			Assert.DoesNotThrow(() => _persister.Update(sagaToUpdate, pathsToIndex));
		}

		[Test, Description("We don't allow two sagas to have the same value of a property that is used to correlate with incoming messages, " +
						   "because that would cause an ambiguity if an incoming message suddenly mathed two or more sagas... " +
						   "moreover, e.g. MongoDB would not be able to handle the message and update multiple sagas reliably because it doesn't have transactions.")]
		public void CannotInsertAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };

			if (_persister is ICanUpdateMultipleSagaDatasAtomically)
			{
				Assert.Ignore("Ignore test as persister does actually support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };
			_persister.Insert(firstSaga, pathsToIndex);

			// act
			// assert
			Assert.Throws<OptimisticLockingException>(() => _persister.Insert(secondSaga, pathsToIndex));
		}

		[Test]
		public void CannotUpdateAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange  
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = "other value" };

			if (_persister is ICanUpdateMultipleSagaDatasAtomically)
				{
				Assert.Ignore("Ignore test as persister does actually support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };
			_persister.Insert(firstSaga, pathsToIndex);
			_persister.Insert(secondSaga, pathsToIndex);

			// act
			// assert
			secondSaga.SomeCorrelationId = theValue;
			Assert.Throws<OptimisticLockingException>(() => _persister.Update(secondSaga, pathsToIndex));
		}

		[Test]
		[Description("This is the opposite of CannotInsertAnotherSagaWithDuplicateCorrelationId, for persistes supporting atomic saga updates.")]
		public void CanInsertAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };

			if (!(_persister is ICanUpdateMultipleSagaDatasAtomically))
			{
				Assert.Ignore("Ignore test as persister does not support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };

			// act
			_persister.Insert(firstSaga, pathsToIndex);
			_persister.Insert(secondSaga, pathsToIndex);

			// assert
		}

		[Test]
		public void CanUpdateAnotherSagaWithDuplicateCorrelationId()
		{
			// arrange  
			var theValue = "this just happens to be the same in two sagas";
			var firstSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = theValue };
			var secondSaga = new SomeSaga { Id = Guid.NewGuid(), SomeCorrelationId = "other value" };

			if (!(_persister is ICanUpdateMultipleSagaDatasAtomically))
			{
				Assert.Ignore("Ignore test as persister does not support multiple saga to be updated automically.");
				return;
			}

			var pathsToIndex = new[] { Reflect.Path<SomeSaga>(s => s.SomeCorrelationId) };

			// act
			_persister.Insert(firstSaga, pathsToIndex);
			_persister.Insert(secondSaga, pathsToIndex);

			secondSaga.SomeCorrelationId = theValue;
			_persister.Update(secondSaga, pathsToIndex);

			// assert
		}

		[Test]
		public void EnsuresUniquenessAlsoOnCorrelationPropertyWithNull()
		{
			var propertyName = Reflect.Path<SomePieceOfSagaData>(d => d.PropertyThatCanBeNull);
			var dataWithIndexedNullProperty = new SomePieceOfSagaData { Id = Guid.NewGuid(), SomeValueWeCanRecognize = "hello" };
			var anotherPieceOfDataWithIndexedNullProperty = new SomePieceOfSagaData { Id = Guid.NewGuid(), SomeValueWeCanRecognize = "hello" };

			_persister.Insert(dataWithIndexedNullProperty, new[] { propertyName });

			Assert.That(
				() => _persister.Insert(anotherPieceOfDataWithIndexedNullProperty, new[] { propertyName }),
				(_persister is ICanUpdateMultipleSagaDatasAtomically) ? (IResolveConstraint)Throws.Nothing : Throws.Exception
			);
		}

		#endregion

		#region Optimistic Concurrency

		[Test]
		public void UsesOptimisticLockingAndDetectsRaceConditionsWhenUpdatingFindingBySomeProperty()
		{
			var indexBySomeString = new[] { "SomeString" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id, SomeString = "hello world!" };
			_persister.Insert(simpleSagaData, indexBySomeString);

			var sagaData1 = _persister.Find<SimpleSagaData>("SomeString", "hello world!");

			Assert.That(sagaData1, Is.Not.Null);

			sagaData1.SomeString = "I changed this on one worker";

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = _persister.Find<SimpleSagaData>("SomeString", "hello world!");
				sagaData2.SomeString = "I changed this on another worker";
				_persister.Update(sagaData2, indexBySomeString);
			}

			Assert.Throws<OptimisticLockingException>(() => _persister.Insert(sagaData1, indexBySomeString));
		}

		[Test]
		public void UsesOptimisticLockingAndDetectsRaceConditionsWhenUpdatingFindingById()
		{
			var indexBySomeString = new[] { "Id" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id, SomeString = "hello world!" };
			_persister.Insert(simpleSagaData, indexBySomeString);

			var sagaData1 = _persister.Find<SimpleSagaData>("Id", id);
			sagaData1.SomeString = "I changed this on one worker";

			using (EnterAFakeMessageContext())
				{
				var sagaData2 = _persister.Find<SimpleSagaData>("Id", id);
				sagaData2.SomeString = "I changed this on another worker";
				_persister.Update(sagaData2, indexBySomeString);
			}

			Assert.Throws<OptimisticLockingException>(() => _persister.Insert(sagaData1, indexBySomeString));
		}

		[Test]
		public void ConcurrentDeleteAndUpdateThrowsOnUpdate()
		{
			var indexBySomeString = new[] { "Id" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id };

			_persister.Insert(simpleSagaData, indexBySomeString);
			var sagaData1 = _persister.Find<SimpleSagaData>("Id", id);
			sagaData1.SomeString = "Some new value";

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = _persister.Find<SimpleSagaData>("Id", id);
				_persister.Delete(sagaData2);
			}

			Assert.Throws<OptimisticLockingException>(() => _persister.Update(sagaData1, indexBySomeString));
		}

		[Test]
		public void ConcurrentDeleteAndUpdateThrowsOnDelete()
		{
			var indexBySomeString = new[] { "Id" };
			var id = Guid.NewGuid();
			var simpleSagaData = new SimpleSagaData { Id = id };

			_persister.Insert(simpleSagaData, indexBySomeString);
			var sagaData1 = _persister.Find<SimpleSagaData>("Id", id);

			using (EnterAFakeMessageContext())
			{
				var sagaData2 = _persister.Find<SimpleSagaData>("Id", id);
				sagaData2.SomeString = "Some new value";
				_persister.Update(sagaData2, indexBySomeString);
			}

			Assert.Throws<OptimisticLockingException>(() => _persister.Delete(sagaData1));
		}

		[Test]
		public void InsertingTheSameSagaDataTwiceGeneratesAnError()
		{
			// arrange
			var sagaDataPropertyPathsToIndex = new[] { Reflect.Path<SimpleSagaData>(d => d.Id) };

			var sagaId = Guid.NewGuid();
			_persister.Insert(new SimpleSagaData { Id = sagaId, Revision = 0, SomeString = "hello!" },
							 sagaDataPropertyPathsToIndex);

			// act
			// assert
			Assert.Throws<OptimisticLockingException>(
				() => _persister.Insert(new SimpleSagaData { Id = sagaId, Revision = 0, SomeString = "hello!" },
									   sagaDataPropertyPathsToIndex));
		}

		#endregion

		#region Update Multiple Sagas Atomically

		[Test]
		public void CanInsertTwoSagasUnderASingleUoW()
		{
			var sagaId1 = Guid.NewGuid();
			var sagaId2 = Guid.NewGuid();

			using (var uow = _manager.Create())
			{
				_persister.Insert(new SimpleSagaData { Id = sagaId1, SomeString = "FirstSaga" }, new[] { "Id" });
				_persister.Insert(new MySagaData { Id = sagaId2, AnotherField = "SecondSaga" }, new[] { "Id" });

				uow.Commit();
			}

			using (EnterAFakeMessageContext())
			{
				var saga1 = _persister.Find<SimpleSagaData>("Id", sagaId1);
				var saga2 = _persister.Find<MySagaData>("Id", sagaId2);

				Assert.That(saga1.SomeString, Is.EqualTo("FirstSaga"));
				Assert.That(saga2.AnotherField, Is.EqualTo("SecondSaga"));
			}
		}

		[Test]
		public void NoChangesAreMadeWhenUoWIsNotCommitted()
		{
			var sagaId1 = Guid.NewGuid();
			var sagaId2 = Guid.NewGuid();

			using (var uow = _manager.Create())
			{
				_persister.Insert(new SimpleSagaData { Id = sagaId1, SomeString = "FirstSaga" }, new[] { "Id" });
				_persister.Insert(new MySagaData { Id = sagaId2, AnotherField = "SecondSaga" }, new[] { "Id" });

				// XXX: Purposedly not committed.
			}

			using (EnterAFakeMessageContext())
			{
				var saga1 = _persister.Find<SimpleSagaData>("Id", sagaId1);
				var saga2 = _persister.Find<MySagaData>("Id", sagaId2);

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

			var savedSagaData = new MySagaData();
			var savedSagaDataId = Guid.NewGuid();
			savedSagaData.Id = savedSagaDataId;
			_persister.Insert(savedSagaData, new string[0]);

			using (_manager.Create())
			//using (_manager.GetScope(autocomplete: true)) //< XXX: May required for something but we don't know when.
			{
				_persister.Find<MySagaData>("Id", savedSagaDataId);


				Assert.Throws<AdoNetSagaLockedException>(() =>
				{
					//using (var thread = new CrossThreadRunner(() =>
					//{
					using (EnterAFakeMessageContext())
					//using (_manager.Create())
					//using (_manager.GetScope(autocomplete: true))
					{
						//_manager.GetScope().Connection.ExecuteCommand("SET TRANSACTION ISOLATION LEVEL  READ COMMITTED;");
						_persister.Find<MySagaData>("Id", savedSagaDataId);
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