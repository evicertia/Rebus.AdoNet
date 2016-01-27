using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Rebus.AdoNet
{
	internal static class IDbCommandExtensions
	{
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
	}
}
