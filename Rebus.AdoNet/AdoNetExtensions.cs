using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
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

			return storage;
		}

		/// <summary>
		/// Configures Rebus to store sagas in AdoNet.
		/// </summary>
		public static AdoNetSagaPersisterFluentConfigurer StoreInAdoNet(this RebusSagasConfigurer configurer, string connectionStringName, string sagaTable, string sagaIndexTable)
		{
			var connString = GetConnectionString(connectionStringName);
			var factory = new AdoNetConnectionFactory(connString.ConnectionString, connString.ProviderName);
			var manager = new AdoNetUnitOfWorkManager(factory);

			configurer.Backbone.ConfigureEvents(x => x.AddUnitOfWorkManager(manager));
			var persister = new AdoNetSagaPersister(manager, sagaTable, sagaIndexTable);

			configurer.Use(persister);

			return persister;
		}

		/// <summary>
		/// Configures Rebus to store timeouts in AdoNet.
		/// </summary>
		public static AdoNetTimeoutStorageFluentConfigurer StoreInAdoNet(this RebusTimeoutsConfigurer configurer, string connectionStringName, string timeoutsTableName)
		{
			var connString = GetConnectionString(connectionStringName);
			var storage = new AdoNetTimeoutStorage(connString.ConnectionString, connString.ProviderName, timeoutsTableName);

			configurer.Use(storage);

			return storage;
		}
	}
}