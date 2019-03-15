using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Rebus;
using Rebus.Bus;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	public interface IAdoNetConnectionFactory
	{
		IDbConnection GetConnection();
		SqlDialect Dialect { get; }
	}

	public class AdoNetConnectionFactory : IAdoNetConnectionFactory
	{
		private readonly DbProviderFactory _factory;
		private readonly string _connectionString;
		private readonly string _providerName;
		private readonly SqlDialect _dialect;

		public SqlDialect Dialect => _dialect;

		public AdoNetConnectionFactory(string connectionString, string providerName)
		{
			_connectionString = connectionString;
			_providerName = providerName;
			_factory = DbProviderFactories.GetFactory(providerName);
			_dialect = SqlDialect.GetDialectFor(_factory, connectionString);

			if (_dialect == null) throw new InvalidOperationException($"Unable to guess dialect for: {connectionString}");

		}

		public IDbConnection GetConnection()
		{
			return _factory.OpenConnection(_connectionString);
		}
	}
}
