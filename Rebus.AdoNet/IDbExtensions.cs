using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Rebus.AdoNet
{
	internal static class IDbExtensions
	{
		#region DbProviderFactory

		public static IDbConnection OpenConnection(this DbProviderFactory @this, string connectionString)
		{
			var connection = @this.CreateConnection();
			connection.ConnectionString = connectionString;
			connection.Open();

			return connection;
		}

		#endregion

		#region IDbConnection Extensions

		public static void ExecuteCommand(this IDbConnection connection, string commandText)
		{
			using (var command = connection.CreateCommand())
			{
				command.CommandText = commandText;
				command.ExecuteNonQuery();
			}
		}

		public static object ExecuteScalar(this IDbConnection connection, string commandText)
		{
			using (var command = connection.CreateCommand())
			{
				command.CommandText = commandText;
				return command.ExecuteScalar();
			}
		}

		public static IDataReader ExecuteReader(this IDbConnection connection, string commandText)
		{
			using (var command = connection.CreateCommand())
			{
				command.CommandText = commandText;
				return command.ExecuteReader();
			}
		}

		#endregion

		#region IDbCommand Extensions
		public static void AddParameter(this IDbCommand command, string name, object value)
		{
			command.AddParameter(name, null, value);
		}

		public static void AddParameter(this IDbCommand command, string name, DbType? type, object value)
		{
			var param = command.CreateParameter();
			param.ParameterName = name;
			param.Value = value;
			param.DbType = type.GetValueOrDefault(param.DbType);
			command.Parameters.Add(param);
		}
		#endregion

		#region IDataReader Extensions
		// Enumerates through the reads in an IDataReader.
		public static IEnumerable<IDataRecord> AsEnumerable(this IDataReader reader)
		{
			while (reader.Read())
			{
				yield return reader;
			}
		}

		// Enumerates through the reads in an IDataReader.
		public static IEnumerable<T> AsEnumerable<T>(this IDataReader reader, string col)
			where T : IConvertible
		{
			while (reader.Read())
			{
				yield return (T)reader[col];
			}
		}
		#endregion
	}
}
