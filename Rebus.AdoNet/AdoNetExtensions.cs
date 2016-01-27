using System.Data;
using System.Data.Common;
using System.Configuration;
using Rebus.Configuration;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Configuration extensions to allow for fluently configuring Rebus with AdoNet.
	/// </summary>
	public static class AdoNetExtensions
	{
		private static ConnectionStringSettings GetConnectionString(string connectionStringName)
		{
			return ConfigurationManager.ConnectionStrings[connectionStringName];
		}

		/// <summary>
		/// Configures Rebus to store subscriptions in AdoNet.
		/// </summary>
		public static AdoNetSubscriptionStorageFluentConfigurer StoreInAdoNet(this RebusSubscriptionsConfigurer configurer, string connectionStringName, string subscriptionsTableName)
		{
			var connString = GetConnectionString(connectionStringName);
			var storage = new AdoNetSubscriptionStorage(connString.ConnectionString, connString.ProviderName, subscriptionsTableName);

			configurer.Use(storage);

			return new AdoNetSubscriptionStorageFluentConfigurer(storage);
		}

		/// <summary>
		/// Configures Rebus to store sagas in AdoNet.
		/// </summary>
		public static AdoNetSagaPersisterFluentConfigurer StoreInAdoNet(this RebusSagasConfigurer configurer, string connectionStringName, string sagaTable, string sagaIndexTable)
		{
			var connString = GetConnectionString(connectionStringName);
			var persister = new AdoNetSagaPersister(connString.ConnectionString, connString.ProviderName, sagaTable, sagaIndexTable);

			configurer.Use(persister);

			return new AdoNetSagaPersisterFluentConfigurer(persister);
		}

		/// <summary>
		/// Configures Rebus to store timeouts in AdoNet.
		/// </summary>
		public static AdoNetTimeoutStorageFluentConfigurer StoreInAdoNet(this RebusTimeoutsConfigurer configurer, string connectionStringName, string timeoutsTableName)
		{
			var connString = GetConnectionString(connectionStringName);
			var storage = new AdoNetTimeoutStorage(connString.ConnectionString, connString.ProviderName, timeoutsTableName);

			configurer.Use(storage);

			return new AdoNetTimeoutStorageFluentConfigurer(storage);
		}

		/// <summary>
		/// Fluent configurer that allows for configuring the underlying <see cref="AdoNetSubscriptionStorageFluentConfigurer"/>
		/// </summary>
		public class AdoNetSubscriptionStorageFluentConfigurer
		{
			readonly AdoNetSubscriptionStorage AdoNetSubscriptionStorage;

			public AdoNetSubscriptionStorageFluentConfigurer(AdoNetSubscriptionStorage AdoNetSubscriptionStorage)
			{
				this.AdoNetSubscriptionStorage = AdoNetSubscriptionStorage;
			}

			/// <summary>
			/// Checks to see if the underlying SQL tables are created - if none of them exist,
			/// they will automatically be created
			/// </summary>
			public AdoNetSubscriptionStorageFluentConfigurer EnsureTableIsCreated()
			{
				AdoNetSubscriptionStorage.EnsureTableIsCreated();

				return this;
			}
		}

		/// <summary>
		/// Fluent configurer that allows for configuring the underlying <see cref="AdoNetSagaPersister"/>
		/// </summary>
		public class AdoNetSagaPersisterFluentConfigurer
		{
			readonly AdoNetSagaPersister AdoNetSagaPersister;

			public AdoNetSagaPersisterFluentConfigurer(AdoNetSagaPersister AdoNetSagaPersister)
			{
				this.AdoNetSagaPersister = AdoNetSagaPersister;
			}

			/// <summary>
			/// Checks to see if the underlying SQL tables are created - if none of them exist,
			/// they will automatically be created
			/// </summary>
			public AdoNetSagaPersisterFluentConfigurer EnsureTablesAreCreated()
			{
				AdoNetSagaPersister.EnsureTablesAreCreated();

				return this;
			}

			/// <summary>
			/// Configures the persister to ignore null-valued correlation properties and not add them to the saga index.
			/// </summary>
			public AdoNetSagaPersisterFluentConfigurer DoNotIndexNullProperties()
			{
				AdoNetSagaPersister.DoNotIndexNullProperties();

				return this;
			}
		}

		/// <summary>
		/// Fluent configurer that allows for configuring the underlying <see cref="AdoNetTimeoutStorageFluentConfigurer"/>
		/// </summary>
		public class AdoNetTimeoutStorageFluentConfigurer
		{
			readonly AdoNetTimeoutStorage AdoNetTimeoutStorage;

			public AdoNetTimeoutStorageFluentConfigurer(AdoNetTimeoutStorage AdoNetTimeoutStorage)
			{
				this.AdoNetTimeoutStorage = AdoNetTimeoutStorage;
			}

			/// <summary>
			/// Checks to see if the underlying SQL tables are created - if none of them exist,
			/// they will automatically be created
			/// </summary>
			public AdoNetTimeoutStorageFluentConfigurer EnsureTableIsCreated()
			{
				AdoNetTimeoutStorage.EnsureTableIsCreated();

				return this;
			}
		}
	}
}