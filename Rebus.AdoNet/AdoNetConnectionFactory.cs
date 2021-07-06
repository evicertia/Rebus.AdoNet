using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Rebus;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	public class AdoNetConnectionFactory
	{
		private static ILog _log;
		private readonly DbProviderFactory _factory;
		private readonly string _connectionString;
		private readonly SqlDialect _dialect;

		public SqlDialect Dialect => _dialect;

		static AdoNetConnectionFactory()
		{
			RebusLoggerFactory.Changed += f => _log = f.GetCurrentClassLogger();
		}

		internal Action<IDbConnection> ConnectionCustomizer { get; set; }

		public AdoNetConnectionFactory(string connectionString, string providerName)
		{
			_connectionString = connectionString;
			_factory = DbProviderFactories.GetFactory(providerName);
			_dialect = SqlDialect.GetDialectFor(_factory, connectionString);

			if (_dialect == null) throw new InvalidOperationException($"Unable to guess dialect for: {connectionString}");

			_log.Info("Created new connection factory for {0}, using dialect {1}.", _factory.GetType().Name, _dialect.GetType().Name);
		}

		public IDbConnection OpenConnection()
		{
			var result = _factory.OpenConnection(_connectionString);
			ConnectionCustomizer?.Invoke(result);
			return result;
		}
	}
}
