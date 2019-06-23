using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace Rebus.AdoNet.Dialects
{
	/// <summary>
	/// PostgreSql (npgsql) exception adapter, required in order
	/// to avoid compile-time dependency on an specific npgsql
	/// assembly version.
	/// </summary>
	public class PostgreSqlExceptionAdapter : DbException
	{
		private readonly DbException _exception;

		#region DbException overrides..

		public override IDictionary Data => _exception.Data;
		public override int ErrorCode => _exception.ErrorCode;
		public override string Message => _exception.Message;
		public override string StackTrace => _exception.StackTrace;

		public string Code => GetPropertyValue<string>("Code", _exception);
		public string ErrorSql => GetPropertyValue<string>("ErrorSql", _exception);

		public override string HelpLink
		{
			get
			{
				return _exception.HelpLink;
			}
			set
			{
				_exception.HelpLink = value;
			}
		}

		public override string Source
		{
			get
			{
				return _exception.Source;
			}
			set
			{
				_exception.Source = value;
			}
		}

		#endregion

		public PostgreSqlExceptionAdapter(DbException exception)
		{
			_exception = exception.ThrowIfNull(nameof(exception));
		}

		public static T GetPropertyValue<T>(string propertyName, object instance)
		{
			var bflags = BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
			return (T)instance.GetType().GetProperty(propertyName, bflags).GetValue(instance, null);
		}

		#region DbException overriden methods..
		public override Exception GetBaseException()
		{
			return _exception.GetBaseException();
		}
		public override bool Equals(object obj)
		{
			return _exception.Equals(obj);
		}
		public override int GetHashCode()
		{
			return _exception.GetHashCode();
		}
		public override void GetObjectData(
			System.Runtime.Serialization.SerializationInfo info, 
			System.Runtime.Serialization.StreamingContext context)
		{
			_exception.GetObjectData(info, context);
		}
		public override string ToString()
		{
			return _exception.ToString();
		}
		#endregion
	}
}
