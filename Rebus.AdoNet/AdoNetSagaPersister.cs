using System;
using System.Data;
using System.Linq;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Serialization.Json;
using Rebus.AdoNet.Schema;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a saga persister for Rebus that stores sagas using an AdoNet provider.
	/// </summary>
	public abstract class AdoNetSagaPersister : IStoreSagaData, AdoNetSagaPersisterFluentConfigurer, ICanUpdateMultipleSagaDatasAtomically
	{
		private const int MaximumSagaDataTypeNameLength = 80;
		private static ILog log;
		private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings {
			TypeNameHandling = TypeNameHandling.All, // TODO: Make it configurable by adding a SagaTypeResolver feature.
			DateFormatHandling = DateFormatHandling.IsoDateFormat, // TODO: Make it configurable..
			Binder = new CustomSerializationBinder()
		};

		private readonly AdoNetUnitOfWorkManager manager;
		private readonly string idPropertyName;
		private Func<Type, string> sagaNameCustomizer;

		protected bool IndexNullProperties { get; private set; } = true;
		protected bool UseSqlArrays { get; private set; }
		protected bool UseSagaLocking { get; private set; }
		protected bool UseNoWaitSagaLocking { get; private set; }

		protected AdoNetUnitOfWorkManager Manager => manager;

		static AdoNetSagaPersister()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the persister with the ability to create connections to database using the specified connection string.
		/// This also means that the persister will manage the connection by itself, closing it when it has stopped using it.
		/// </summary>
		public AdoNetSagaPersister(AdoNetUnitOfWorkManager manager)
		{
			this.manager = manager;
			this.idPropertyName = Reflect.Path<ISagaData>(x => x.Id);
		}

		#region AdoNetSagaPersisterFluentConfigurer

		/// <summary>
		/// Configures the persister to ignore null-valued correlation properties and not add them to the saga index.
		/// </summary>
		public AdoNetSagaPersisterFluentConfigurer DoNotIndexNullProperties()
		{
			IndexNullProperties = false;
			return this;
		}

		public AdoNetSagaPersisterFluentConfigurer UseLockingOnSagaUpdates(bool waitForLocks)
		{
			UseSagaLocking = true;
			UseNoWaitSagaLocking = !waitForLocks;
			return this;
		}

		/// <summary>
		/// Creates the necessary saga storage tables if they haven't already been created. If a table already exists
		/// with a name that matches one of the desired table names, no action is performed (i.e. it is assumed that
		/// the tables already exist).
		/// </summary>
		public virtual AdoNetSagaPersisterFluentConfigurer EnsureTablesAreCreated()
		{
			throw new NotImplementedException("Creation of Saga tables not implemented.");
		}

		/// <summary>
		/// Customizes the saga names by invoking this customizer.
		/// </summary>
		/// <param name="customizer">The customizer.</param>
		/// <returns></returns>
		public AdoNetSagaPersisterFluentConfigurer CustomizeSagaNamesAs(Func<Type, string> customizer)
		{
			this.sagaNameCustomizer = customizer;
			return this;
		}

		/// <summary>
		/// Enables locking of sagas as to avoid two or more workers to update them concurrently.
		/// </summary>
		/// <returns>The saga locking.</returns>
		public AdoNetSagaPersisterFluentConfigurer EnableSagaLocking()
		{
			UseSagaLocking = true;
			return this;
		}

		/// <summary>
		/// Uses the use of sql array types for storing indexes related to correlation properties.
		/// </summary>
		/// <returns>The sql arrays.</returns>
		public AdoNetSagaPersisterFluentConfigurer UseSqlArraysForCorrelationIndexes()
		{
			UseSqlArrays = true;
			return this;
		}

		/// <summary>
		/// Allows customizing opened connections by passing a delegate/lambda to invoke for each new connection.
		/// </summary>
		/// <param name="customizer"></param>
		/// <returns></returns>
		public AdoNetSagaPersisterFluentConfigurer CustomizeOpenedConnections(Action<IDbConnection> customizer)
		{
			manager.ConnectionFactory.ConnectionCustomizer = customizer;
			return this;
		}

		/// <summary>
		/// Customizes type2name & name2type mapping logic used during serialization/deserialization.
		/// </summary>
		/// <param name="nameToTypeResolver">Delegate to invoke when resolving a name-to-type during deserialization.</param>
		/// <param name="typeToNameResolver">Delegate to invoke when resolving a type-to-name during serialization.</param>
		/// <returns></returns>
		public AdoNetSagaPersisterFluentConfigurer CustomizeSerializationTypeResolving(Func<TypeDescriptor, Type> nameToTypeResolver, Func<Type, TypeDescriptor> typeToNameResolver)
		{
			(Settings.Binder as CustomSerializationBinder).NameToTypeResolver = nameToTypeResolver;
			(Settings.Binder as CustomSerializationBinder).TypeToNameResolver = typeToNameResolver;
			return this;
		}

		#endregion
		
		#region Saga TypeName mangling

		private static string GetClassName(Type type)
		{
			var classNameRegex = new Regex("^[a-zA-Z0-9_]*[\\w]", RegexOptions.IgnoreCase);
			var match = classNameRegex.Match(type.Name);

			if (!match.Success) throw new Exception($"Error trying extract name class from type: {type.Name}");

			return match.Value;
		}

		private static IEnumerable<string> GetGenericArguments(Type type)
		{
			return type.GetGenericArguments()
				.Select(x => x.Name)
				.ToList();
		}

		private static string GetDefaultSagaName(Type type)
		{
			var declaringType = type.DeclaringType;

			if (type.IsNested && declaringType != null)
			{
				if (declaringType.IsGenericType)
				{
					var className = GetClassName(declaringType);
					var genericArguments = GetGenericArguments(type).ToList();

					return genericArguments.Any()
						? $"{className}<{string.Join(",", genericArguments)}>"
						: $"{className}";
				}

				return declaringType.Name;
			}

			return type.Name;
		}
		
		protected string GetSagaTypeName(Type sagaDataType)
		{
			var sagaTypeName = sagaNameCustomizer != null ? sagaNameCustomizer(sagaDataType) : GetDefaultSagaName(sagaDataType);

			if (sagaTypeName.Length > MaximumSagaDataTypeNameLength)
			{
				throw new InvalidOperationException(
					string.Format(
						@"Sorry, but the maximum length of the name of a saga data class is currently limited to {0} characters!

This is due to a limitation in SQL Server, where compound indexes have a 900 byte upper size limit - and
since the saga index needs to be able to efficiently query by saga type, key, and value at the same time,
there's room for only 200 characters as the key, 200 characters as the value, and 80 characters as the
saga type name.",
						MaximumSagaDataTypeNameLength));
			}

			return sagaTypeName;
		}
		
		#endregion

		protected string Serialize(ISagaData sagaData)
		{
			return JsonConvert.SerializeObject(sagaData, Formatting.Indented, Settings);
		}

		public abstract void Insert(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex);

		public abstract void Update(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex);

		public abstract void Delete(ISagaData sagaData);

		protected abstract string Fetch<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage)
			where TSagaData : class, ISagaData;

		public TSagaData Find<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage)
			where TSagaData : class, ISagaData
		{
			var sagaType = GetSagaTypeName(typeof(TSagaData));

			log.Debug("Finding saga of type {0} with {1} = {2}", sagaType, sagaDataPropertyPath, fieldFromMessage);

			var data = Fetch<TSagaData>(sagaDataPropertyPath, fieldFromMessage);
		
			if (data == null)
			{
				log.Debug("No saga found of type {0} with {1} = {2}", sagaType, sagaDataPropertyPath, fieldFromMessage);
				return null;
			}

			log.Debug("Found saga of type {0} with {1} = {2}", sagaType, sagaDataPropertyPath, fieldFromMessage);

			try
			{
				return JsonConvert.DeserializeObject<TSagaData>(data, Settings);
			}
			catch { }

			try
			{
				return (TSagaData)JsonConvert.DeserializeObject(data, Settings);
			}
			catch (Exception exception)
			{
				var message = string.Format("An error occurred while attempting to deserialize '{0}' into a {1}", data,
					typeof(TSagaData));

				throw new ApplicationException(message, exception);
			}
		}
	}
}
