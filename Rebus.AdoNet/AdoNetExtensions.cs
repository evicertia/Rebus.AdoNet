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
			var factory = new AdoNetConnectionFactory(connString.ConnectionString, connString.ProviderName);
			var storage = new AdoNetSubscriptionStorage(factory, subscriptionsTableName);

			configurer.Use(storage);

			return storage;
		}

		/// <summary>
		/// Configures Rebus to store sagas in AdoNet.
		/// </summary>
		public static AdoNetSagaPersisterFluentConfigurer StoreInAdoNet(
			this RebusSagasConfigurer configurer,
			string connectionStringName,
			Func<AdoNetUnitOfWorkManager, AdoNetSagaPersister> persisterCreator,
			Func<AdoNetConnectionFactory, IMessageContext, IAdoNetUnitOfWork> unitOfWorkCreator = null)
		{
			Guard.NotNull(() => persisterCreator, persisterCreator);
			if (unitOfWorkCreator == null) unitOfWorkCreator = (fact, cont) => new AdoNetUnitOfWork(fact, cont);

			var connString = GetConnectionString(connectionStringName);
			var factory = new AdoNetConnectionFactory(connString.ConnectionString, connString.ProviderName);
			var manager = new AdoNetUnitOfWorkManager(factory, unitOfWorkCreator);

			configurer.Backbone.ConfigureEvents(x => x.AddUnitOfWorkManager(manager));
			var persister = persisterCreator(manager);

			configurer.Use(persister);

			return persister;
		}

		/// <summary>
		/// Configures Rebus to store sagas in AdoNet, using the new (advanced) persister using json columns/indexes.
		/// </summary>
		public static AdoNetSagaPersisterFluentConfigurer StoreInAdoNet(
			this RebusSagasConfigurer configurer,
			string connectionStringName,
			string sagasTableName,
			Func<AdoNetConnectionFactory, IMessageContext, IAdoNetUnitOfWork> unitOfWorkCreator = null)
		{
			return configurer.StoreInAdoNet(connectionStringName,
				x => new AdoNetSagaPersisterAdvanced(x, sagasTableName),
				unitOfWorkCreator);
		}
		
		/// <summary>
		/// Configures Rebus to store sagas in AdoNet, using the legacy persister (for backwards compat).
		/// </summary>
		public static AdoNetSagaPersisterFluentConfigurer StoreInAdoNetUsingLegacyPersister(
			this RebusSagasConfigurer configurer,
			string connectionStringName,
			string sagasTableName, string sagasIndexTableName,
			Func<AdoNetConnectionFactory, IMessageContext, IAdoNetUnitOfWork> unitOfWorkCreator = null)
		{
			return configurer.StoreInAdoNet(connectionStringName,
				x => new AdoNetSagaPersisterLegacy(x, sagasTableName, sagasIndexTableName),
				unitOfWorkCreator);
		}

		/// <summary>
		/// Configures Rebus to store timeouts in AdoNet.
		/// </summary>
		public static AdoNetTimeoutStorageFluentConfigurer StoreInAdoNet(this RebusTimeoutsConfigurer configurer, string connectionStringName, string timeoutsTableName)
		{
			var connString = GetConnectionString(connectionStringName);
			var factory = new AdoNetConnectionFactory(connString.ConnectionString, connString.ProviderName);
			var storage = new AdoNetTimeoutStorage(factory, timeoutsTableName);

			configurer.Use(storage);

			return storage;
		}

		/// <summary>
		/// Configures Rebus to store timeouts in AdoNet. 
		/// Use batchSize for limit the number of timeouts that you get from database.
		/// </summary>
		public static AdoNetTimeoutStorageFluentConfigurer StoreInAdoNet(this RebusTimeoutsConfigurer configurer, string connectionStringName, string timeoutsTableName, uint batchsize)
		{
			var connString = GetConnectionString(connectionStringName);
			var factory = new AdoNetConnectionFactory(connString.ConnectionString, connString.ProviderName);
			var storage = new AdoNetTimeoutStorage(factory, timeoutsTableName, batchsize);

			configurer.Use(storage);

			return storage;
		}
	}
}