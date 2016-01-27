using System;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Rebus.AdoNet
{
	// TODO: Make this configurable/extensible..
	public class AdoNetExceptionManager
	{
		public static bool IsOptimistickLockingException(Exception ex)
		{
			return (ex is DbException);
		}
	}
}
