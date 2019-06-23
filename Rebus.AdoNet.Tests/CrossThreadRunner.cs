using System;
using System.Reflection;
using System.Security.Permissions;
using System.Threading;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Based on http://www.peterprovost.org/blog/2004/11/03/Using-CrossThreadTestRunner/
	/// </summary>
	class CrossThreadRunner : IDisposable
	{
		private readonly Thread thread;
		private readonly ThreadStart start;
		private Exception lastException;
		private volatile bool _disposed;

		private const string RemoteStackTraceFieldName = "_remoteStackTraceString";
		private static readonly FieldInfo RemoteStackTraceField = typeof(Exception).GetField(RemoteStackTraceFieldName, BindingFlags.Instance | BindingFlags.NonPublic);

		public CrossThreadRunner(ThreadStart start)
		{
			this.start = start;
			this.thread = new Thread(Start);
			this.thread.SetApartmentState(ApartmentState.STA);
		}

		[ReflectionPermission(SecurityAction.Demand)]
		private static void ThrowExceptionPreservingStack(Exception exception)
		{
			if (RemoteStackTraceField != null)
			{
				RemoteStackTraceField.SetValue(exception, exception.StackTrace + Environment.NewLine);
			}
			throw exception;
		}

		private void Start()
		{
			try
			{
				start.Invoke();
			}
			catch (Exception e)
			{
				lastException = e;
			}
		}

		public void Run()
		{
			lastException = null;
			thread.Start();
		}

		public void Dispose()
		{
			if (!_disposed)
			{
				thread.Join();
				_disposed = true;

				if (lastException != null)
				{
					ThrowExceptionPreservingStack(lastException);
				}
			}
		}
	}
}
