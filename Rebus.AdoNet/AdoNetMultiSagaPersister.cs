using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.AdoNet
{
	public class AdoNetMultiSagaPersister : AdoNetSagaPersister, ICanUpdateMultipleSagaDatasAtomically
	{
		/// <summary>
		/// Constructs the persister with the ability to create connections to database using the specified connection string.
		/// This also means that the persister will manage the connection by itself, closing it when it has stopped using it.
		/// </summary>
		public AdoNetMultiSagaPersister(string connectionString, string providerName, string sagaTableName, string sagaIndexTableName)
			: base(connectionString, providerName, sagaTableName, sagaIndexTableName)
		{
		}

		/// <summary>
		/// Constructs the persister with the ability to use an externally provided <see cref="IDbConnection"/>, thus allowing it
		/// to easily enlist in any ongoing SQL transaction magic that might be going on. This means that the perister will assume
		/// that someone else manages the connection's lifetime.
		/// </summary>
		public AdoNetMultiSagaPersister(Func<ConnectionHolder> connectionFactoryMethod, string sagaTableName, string sagaIndexTableName)
			: base(connectionFactoryMethod, sagaTableName, sagaIndexTableName)
		{
		}


	}
}
