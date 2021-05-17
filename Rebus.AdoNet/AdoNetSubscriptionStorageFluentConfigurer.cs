using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Fluent configurer that allows for configuring the underlying <see cref="AdoNetSubscriptionStorageFluentConfigurer"/>
	/// </summary>
	public interface AdoNetSubscriptionStorageFluentConfigurer
	{
		/// <summary>
		/// Checks to see if the underlying SQL tables are created - if none of them exist,
		/// they will automatically be created
		/// </summary>
		AdoNetSubscriptionStorageFluentConfigurer EnsureTableIsCreated();

		/// <summary>
		/// Customizes opened IDbConnections before usage.
		/// </summary>
		/// <param name="customizer">Delegate to invoke for each opened IDbConnection</param>
		/// <returns></returns>
		AdoNetSubscriptionStorageFluentConfigurer CustomizeOpenedConnections(Action<IDbConnection> customizer);
	}
}
