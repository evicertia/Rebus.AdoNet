using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

using Rebus.Serialization.Json;

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

		/// <summary>
		/// Enables locking of sagas as to avoid two or more workers to update them concurrently.
		/// </summary>
		/// <returns>The saga locking.</returns>
		AdoNetSagaPersisterFluentConfigurer EnableSagaLocking();

		/// <summary>
		/// Uses the use of sql array types for storing indexes related to correlation properties.
		/// </summary>
		/// <returns>The sql arrays.</returns>
		AdoNetSagaPersisterFluentConfigurer UseSqlArraysForCorrelationIndexes();

		/// <summary>
		/// Customizes opened IDbConnections before usage.
		/// </summary>
		/// <param name="customizer">Delegate to invoke for each opened IDbConnection</param>
		/// <returns></returns>
		AdoNetSagaPersisterFluentConfigurer CustomizeOpenedConnections(Action<IDbConnection> customizer);

		/// <summary>
		/// Customizes type2name & name2type mapping logic used during serialization/deserialization.
		/// </summary>
		/// <param name="nameToTypeResolver">Delegate to invoke when resolving a name-to-type during deserialization.</param>
		/// <param name="typeToNameResolver">Delegate to invoke when resolving a type-to-name during serialization.</param>
		/// <returns></returns>
		AdoNetSagaPersisterFluentConfigurer CustomizeSerializationTypeResolving(Func<TypeDescriptor, Type> nameToTypeResolver, Func<Type, TypeDescriptor> typeToNameResolver);
	}
}
