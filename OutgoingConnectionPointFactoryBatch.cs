using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESB_ConnectionPoints.PluginsInterfaces;
using ESB_ConnectionPoints.Utils;

namespace ESB_ConnetionPoints.BatchTransmission
{
    public sealed class OutgoingConnectionPointFactoryBatch : IOutgoingConnectionPointFactory
    {
        public IOutgoingConnectionPoint Create(
          Dictionary<string, string> parameters,
          IServiceLocator serviceLocator)
        {
            return (IOutgoingConnectionPoint)new OutgoingConnectionPointBatch(parameters.GetStringParameter("Настройки в формате JSON", true, ""), serviceLocator);
        }
    }
}
