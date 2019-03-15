using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

using Rebus.Logging;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a subscription storage for Rebus that stores sagas in AdoNet.
	/// </summary>
	public class AdoNetSubscriptionStorage : IStoreSubscriptions, AdoNetSubscriptionStorageFluentConfigurer
	{
		static ILog log;

		readonly string subscriptionsTableName;
		readonly IAdoNetConnectionFactory factory;

		static AdoNetSubscriptionStorage()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		public AdoNetSubscriptionStorage(IAdoNetConnectionFactory factory, string subscriptionsTableName)
		{
			this.factory = factory;
			this.subscriptionsTableName = subscriptionsTableName;
		}

		public string SubscriptionsTableName
		{
			get { return subscriptionsTableName; }
		}

		/// <summary>
		/// Stores a subscription for the given message type and the given subscriber endpoint in the underlying SQL table.
		/// </summary>
		public void Store(Type eventType, string subscriberInputQueue)
		{

			using (var connection = factory.GetConnection())
			using (var command = connection.CreateCommand())
			{
				const string Sql = @"insert into ""{0}"" (""message_type"", ""endpoint"") values (@message_type, @endpoint)";

				command.CommandText = string.Format(Sql, subscriptionsTableName);

				command.AddParameter("message_type", eventType.FullName);
				command.AddParameter("endpoint", subscriberInputQueue);

				try
				{
					command.ExecuteNonQuery();
				}
				catch (DbException ex)
				{
					if (!AdoNetExceptionManager.IsDuplicatedKeyException(ex))
						throw;
				}
			}
		}

		/// <summary>
		/// Removes the subscription (if any) for the given message type and subscriber endpoint from the underlying SQL table.
		/// </summary>
		public void Remove(Type eventType, string subscriberInputQueue)
		{
			const string Sql = @"delete from ""{0}"" where ""message_type"" = @message_type and ""endpoint"" = @endpoint";

			using (var connection = factory.GetConnection())
			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(Sql, subscriptionsTableName);

				command.AddParameter("message_type", eventType.FullName);
				command.AddParameter("endpoint", subscriberInputQueue);

				command.ExecuteNonQuery();
			}
		}

		/// <summary>
		/// Queries the underlying table for subscriber endpoints that are subscribed to the given message type.
		/// </summary>
		public string[] GetSubscribers(Type eventType)
		{
			using (var connection = factory.GetConnection())
			using (var command = connection.CreateCommand())
			{
				const string Sql = @"select ""endpoint"" from ""{0}"" where ""message_type"" = @message_type";

				command.CommandText = string.Format(Sql, subscriptionsTableName);

				command.AddParameter("message_type", eventType.FullName);

				var endpoints = new List<string>();

				using (var reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						endpoints.Add((string)reader["endpoint"]);
					}
				}

				return endpoints.ToArray();
			}
		}

		/// <summary>
		/// Creates the necessary subscripion storage table if it hasn't already been created. If a table already exists
		/// with a name that matches the desired table name, no action is performed (i.e. it is assumed that
		/// the table already exists).
		/// </summary>
		public AdoNetSubscriptionStorageFluentConfigurer EnsureTableIsCreated()
		{
			using (var connection = factory.GetConnection())
			{
				var tableNames = factory.Dialect.GetTableNames(connection);

				if (tableNames.Contains(subscriptionsTableName, StringComparer.OrdinalIgnoreCase))
				{
					return this;
				}

				log.Info("Table '{0}' does not exist - it will be created now", subscriptionsTableName);

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(@"
CREATE TABLE ""{0}"" (
	""message_type"" VARCHAR(200) NOT NULL,
	""endpoint"" VARCHAR(200) NOT NULL,
	PRIMARY KEY (""message_type"", ""endpoint"")
);
", subscriptionsTableName);
					command.ExecuteNonQuery();
				}
			}

			return this;
		}
	}
}