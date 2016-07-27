using System;
using System.IO;

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
		#endregion

		private static readonly ILog _Log = LogManager.GetLogger<SagaPersisterTests>();
		private const string PROVIDER_NAME = "csharp-sqlite";
		private const string CONNECTION_STRING = @"Data Source=file://{0};Version=3;New=True;";

		private AdoNetSagaPersister _persister;

		public SagaPersisterTests()
			: base(GetConnectionString(), PROVIDER_NAME)
		{
		}

		private static string GetConnectionString()
		{
			var dbfile = AssemblyFixture.TrackDisposable(new TempFile());
			File.Delete(dbfile.Path);
			_Log.DebugFormat("Using temporal file: {0}", dbfile.Path);
			return string.Format(CONNECTION_STRING, dbfile.Path);
		}

		protected override void OnSetUp()
		{
			DropSagaTables();
			_persister = new AdoNetSagaPersister(ConnectionString, ProviderName, SagaIndexTableName, SagaTableName);
		}

		[Test]
		public void WhenIgnoringNullProperties_DoesNotSaveNullPropertiesOnUpdate()
		{
			_persister.EnsureTablesAreCreated()
				.DoNotIndexNullProperties();

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
			_persister.EnsureTablesAreCreated()
				.DoNotIndexNullProperties();

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

			_persister.Insert(firstPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			// must not throw:
			_persister.Insert(nextPieceOfSagaDataWithNullValueOnProperty, correlationPropertyPaths);

			var firstPiece = _persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty1);
			var nextPiece = _persister.Find<PieceOfSagaData>(Reflect.Path<PieceOfSagaData>(s => s.SomeProperty), correlationProperty2);

			Assert.That(firstPiece.Id, Is.EqualTo(firstId));
			Assert.That(nextPiece.Id, Is.EqualTo(nextId));
		}

		[Test]
		public void CanCreateSagaTablesAutomatically()
		{
			// arrange

			// act
			_persister.EnsureTablesAreCreated();

			// assert
			var existingTables = GetTableNames();
			Assert.That(existingTables, Contains.Item(SagaIndexTableName));
			Assert.That(existingTables, Contains.Item(SagaTableName));
		}

		[Test]
		public void DoesntDoAnythingIfTheTablesAreAlreadyThere()
		{
			// arrange
			ExecuteCommand(@"CREATE TABLE """ + SagaTableName + @""" (""id"" INT NOT NULL)");
			ExecuteCommand(@"CREATE TABLE """ + SagaIndexTableName + @""" (""id"" INT NOT NULL)");

			// act
			// assert
			_persister.EnsureTablesAreCreated();
			_persister.EnsureTablesAreCreated();
			_persister.EnsureTablesAreCreated();
		}
	}
}