using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;

using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Provides an opened and ready-to-use <see cref="NpgsqlConnection"/> for doing stuff in SQL Server.
	/// </summary>
	public class ConnectionHolder : IDisposable
	{
		/// <summary>
		/// Gets the SQL dialect of the current connection.
		/// </summary>
		public SqlDialect Dialect { get; private set; }

		/// <summary>
		/// Gets the current open connection to the database
		/// </summary>
		public IDbConnection Connection { get; private set; }

		/// <summary>
		/// Gets the currently ongoing transaction (or null if operating in non-transactional mode)
		/// </summary>
		public IDbTransaction Transaction { get; private set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="ConnectionHolder"/> class.
		/// </summary>
		/// <param name="connection">The connection.</param>
		/// <param name="transaction">The transaction.</param>
		internal ConnectionHolder(IDbConnection connection, SqlDialect dialect, IDbTransaction transaction)
		{
			if (connection == null) throw new ArgumentNullException(nameof(connection));
			if (dialect == null) throw new ArgumentNullException(nameof(dialect));

			Connection = connection;
			Dialect = dialect;
			Transaction = transaction;
		}

		/// <summary>
		/// Constructs a <see cref="ConnectionHolder"/> instance with the given connection. The connection
		/// will be used for non-transactional work
		/// </summary>
		public static ConnectionHolder ForNonTransactionalWork(IDbConnection connection, SqlDialect dialect)
		{
			if (connection == null) throw new ArgumentNullException(nameof(connection));
			if (dialect == null) throw new ArgumentNullException(nameof(dialect));

			return new ConnectionHolder(connection, dialect, null);
		}

		/// <summary>
		/// Constructs a <see cref="ConnectionHolder"/> instance with the given connection and transaction. The connection
		/// will be used for transactional work
		/// </summary>
		public static ConnectionHolder ForTransactionalWork(IDbConnection connection, SqlDialect dialect, IDbTransaction transaction)
		{
			if (connection == null) throw new ArgumentNullException(nameof(connection));
			if (dialect == null) throw new ArgumentNullException(nameof(dialect));
			if (transaction == null) throw new ArgumentNullException(nameof(transaction));

			return new ConnectionHolder(connection, dialect, transaction);
		}

		/// <summary>
		/// Creates a new <see cref="NpgsqlCommand"/>, setting the transaction if necessary
		/// </summary>
		public IDbCommand CreateCommand()
		{
			var sqlCommand = Connection.CreateCommand();

			if (Transaction != null)
			{
				sqlCommand.Transaction = Transaction;
			}

			return sqlCommand;
		}

		/// <summary>
		/// Ensures that the ongoing transaction is disposed and the held connection is disposed
		/// </summary>
		public void Dispose()
		{
			if (Transaction != null)
			{
				Transaction.Dispose();
			}

			Connection.Dispose();
		}

		/// <summary>
		/// Commits the transaction if one is present
		/// </summary>
		public void Commit()
		{
			if (Transaction == null) return;

			Transaction.Commit();
		}

		/// <summary>
		/// Rolls back the transaction is one is present
		/// </summary>
		public void Rollback()
		{
			if (Transaction == null) return;

			Transaction.Rollback();
		}

		/// <summary>
		/// Queries sys.Tables in the current DB
		/// </summary>
		public IEnumerable<string> GetTableNames()
		{
			return Dialect.GetTableNames(Connection);
		}
	}
}