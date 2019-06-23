using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Fluent configurer that allows for configuring the underlying <see cref="AdoNetSagaPersister"/>
	/// </summary>
	public interface AdoNetSagaPersisterFluentConfigurer
	{
		/// <summary>
		/// Checks to see if the underlying SQL tables are created - if none of them exist,
		/// they will automatically be created
		/// </summary>
		AdoNetSagaPersisterFluentConfigurer EnsureTablesAreCreated();

		/// <summary>
		/// Configures the persister to ignore null-valued correlation properties and not add them to the saga index.
		/// </summary>
		AdoNetSagaPersisterFluentConfigurer DoNotIndexNullProperties();

		/// <summary>
		/// Uses locking when retrieving/updating saga data rows from database.
		/// </summary>
		/// <returns></returns>
		/// <param name="waitForLocks">If set to <c>true</c> wait for locks.</param>
		AdoNetSagaPersisterFluentConfigurer UseLockingOnSagaUpdates(bool waitForLocks);

		/// <summary>
		/// Customizes the saga names using this customizer.
		/// </summary>
		/// <param name="customizer">The customizer.</param>
		/// <returns></returns>
		AdoNetSagaPersisterFluentConfigurer CustomizeSagaNamesAs(Func<Type, string> customizer);
	}
}
