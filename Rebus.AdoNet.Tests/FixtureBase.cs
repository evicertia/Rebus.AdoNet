using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;
using Rebus.Logging;
using Rebus.Testing;
using Rhino.Mocks;

namespace Rebus.AdoNet
{
	public abstract class FixtureBase
	{
		protected DisposableTracker Disposables { get; }

		public FixtureBase()
		{
			Disposables = new DisposableTracker();
		}

		[SetUp]
		public void SetUp()
		{
			//TimeMachine.Reset();
			FakeMessageContext.Reset();
			RebusLoggerFactory.Reset();
		}

		[TearDown]
		public void TearDown()
		{
			Disposables.DisposeTheDisposables();
		}
	}
}
