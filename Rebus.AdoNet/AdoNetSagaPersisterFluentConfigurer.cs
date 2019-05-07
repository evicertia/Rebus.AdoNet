﻿using System;
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
	}
}
