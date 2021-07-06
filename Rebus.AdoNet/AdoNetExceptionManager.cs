using System;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Rebus.AdoNet
{
	// TODO: Make this configurable/extensible..
	// TODO: Provide a delegate the user can customize as to convert 
	//		 exceptions into DBConcurrencyException, which should be
	//		 what AdoNet's code should try to catch.
	public class AdoNetExceptionManager
	{
		public static bool IsDuplicatedKeyException(Exception ex)
		{
			// FIXME: This is too dummy.
			return (ex is DbException);
		}


	}
}
