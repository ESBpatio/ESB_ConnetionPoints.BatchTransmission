using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using ESB_ConnectionPoints.PluginsInterfaces;
using ESB_ConnectionPoints.Utils;
using Newtonsoft.Json.Linq;
using NJsonSchema.Validation;


namespace ESB_ConnetionPoints.BatchTransmission
{
    public class OutgoingConnectionPointBatch : IOutgoingConnectionPoint, IStandartOutgoingConnectionPoint
    {
        private readonly ILogger mainLogger;
        private readonly IMessageFactory messageFactory;
        private Dictionary<string, Message> messageInStorage = new Dictionary<string, Message>();
        private Dictionary<string, int> Pool = new Dictionary<string, int>();
        private Dictionary<Guid, Guid> messageWait = new Dictionary<Guid, Guid>();
        private DateTime oldTime;
        private bool isDebugMode;
        private int messageToPackage;
        private string classMessage;
        private string typeMessage;
        private int timeWait;


        public OutgoingConnectionPointBatch(string jsonSettings, IServiceLocator serviceLocator)
        {
            this.mainLogger = serviceLocator.GetLogger(this.GetType());
            this.messageFactory = serviceLocator.GetMessageFactory();
            this.parseSettings(jsonSettings);
        }

        private void parseSettings(string jsonSettings)
        {
            if (string.IsNullOrEmpty(jsonSettings))
            {
                throw new Exception("Не задан параметр <jsonSettings>");
            }
            JObject jObject;
            try
            {
                jObject = JObject.Parse(jsonSettings);
            }
            catch (Exception ex)
            {

                throw new Exception("Не удалось разобрать строку настроек JSON! " + ex.Message);
            }
            ICollection<ValidationError> validationError;
            //if (!JSONUtils.IsValid(jsonSettings, "package-server-connection-point-settings-schema.json", out validationError))
            //    throw new ArgumentException("В настройках обнаружены ошибки:\n" + string.Join<ValidationError>("\n", (IEnumerable<ValidationError>)validationError));
            this.messageToPackage = (int)jObject["MessageToPackage"];//JSONUtils.IntValue(jObject, "MessageToPackage", 1);
            this.isDebugMode = (bool)jObject["DebugMode"];//JSONUtils.BoolValue(jObject, "DebugMode", false);
            this.classMessage = (string)jObject["ClassId"];//JSONUtils.StringValue(jObject, "ClassId", "");
            this.typeMessage = (string)jObject["Type"];//JSONUtils.StringValue(jObject, "Type", "DTP");
            this.timeWait = (int)jObject["WaitingTime"];//JSONUtils.IntValue(jObject, "WaitingTime", 1);

        }

        public void Cleanup()
        {

        }

        public void Dispose()
        {

        }

        public void Initialize()
        {
            foreach (var pool in Pool)
            {
                Pool.Remove(pool.Key);
            }
            foreach (var storage in messageInStorage)
            {
                messageInStorage.Remove(storage.Key);
            }
            oldTime = DateTime.Now;
            if (this.isDebugMode)
                mainLogger.Debug(string.Format("Проверка настроек в режиме отладки:\n" +
                    "Максимальное количество сообщений для формирования пакета {0}\n" +
                    "Класс входящего сообщения: {1}\n" +
                    "Тип входящего сообщения: {2}\n" +
                    "Время ожидания формирования пакета {3}", this.messageToPackage, this.classMessage, this.typeMessage, this.timeWait));
        }

        public void Run(IMessageSource messageSource, IMessageReplyHandler replyHandler, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    //Отправка по времени
                    if (oldTime.AddMinutes(this.timeWait) <= DateTime.Now)
                    {
                        SendBatchMessage(replyHandler, messageSource, true, (Message)null);
                        oldTime = DateTime.Now;
                    }
                    //Получения сообщения из очереди
                    Message message = (Message)null;
                    message = messageSource.PeekLockMessage(ct, 10000);


                    if (!(message == (Message)null))
                    {
                        if(message.Type == "blackhole")
                        {
                            messageSource.CompletePeekLock(message.Id);
                            continue;
                        }
                        Logger logger = new Logger(this.mainLogger, this.isDebugMode, string.Format("Сообщение {0}", (object)message.Id));
                        createBatchMessage(message, messageSource, logger);

                        //Отправка по условию количество в пакете
                        if (Pool[message.ClassId] >= this.messageToPackage)
                        {
                            SendBatchMessage(replyHandler, messageSource, false, message);
                        }
                    }
                    ct.WaitHandle.WaitOne(10);
                }
                catch (Exception ex)
                {
                    mainLogger.Error("Ошибка в потоке : " + ex.Message);
                }
            }
        }
        //Хранения сообщений по определенному классу.
        public void messageStorage(XDocument xDocument, string classId, out Guid Id)
        {
            Message message = new Message();
            message.ClassId = this.classMessage;
            message.Body = Encoding.UTF8.GetBytes(xDocument.ToString());
            message.Id = Guid.NewGuid();
            message.Type = this.typeMessage;
            message.SetPropertyWithValue("КлассОдиночногоСообщения", classId);
            messageInStorage.Add(classId, message);
            countMessage(classId);
            Id = message.Id;
        }
        //Создание пакета сообщения.
        public void createBatchMessage(Message message, IMessageSource messageSource, Logger logger)
        {
            try
            {
                if (!(messageInStorage.ContainsKey(message.ClassId)))
                {

                    //Создание новой xml
                    XDocument xDocument = new XDocument();
                    //Корневой элемент новой xml
                    XElement root = new XElement("classData");
                    XElement row = new XElement("row");
                    //Создаем элементы дерева
                    root.Add(new XElement("ТипОбъекта"));
                    root.Add(new XElement("КоличествоСообщений"));
                    root.Add(new XElement("Сообщения"));
                    root.Element("Сообщения").Add(row);
                    root.Element("КоличествоСообщений").Value = "1";
                    if (message.HasProperty("ТипОбъекта"))
                        root.Element("ТипОбъекта").Value = message.GetPropertyValue<string>("ТипОбъекта");

                    var newElement = new XElement("Сообщение");
                    newElement.Value = Encoding.UTF8.GetString(message.Body);

                    root.Element("Сообщения").Element("row").Add(newElement);

                    xDocument.Add(root);

                    messageStorage(xDocument, message.ClassId, out Guid Id);
                    messageWait.Add(message.Id, Id);
                }
                else
                {
                    Message storageMessage = messageInStorage[message.ClassId];
                    XDocument xDocument = XDocument.Parse(Encoding.UTF8.GetString(storageMessage.Body));

                    XElement row = new XElement("row");
                    var newElement = new XElement("Сообщение");
                    newElement.Value = Encoding.UTF8.GetString(message.Body);
                    row.Add(newElement);

                    xDocument.Element("classData").Element("КоличествоСообщений").Value = (int.Parse(xDocument.Element("classData").Element("КоличествоСообщений").Value) + 1).ToString();
                    xDocument.Element("classData").Element("Сообщения").Add(row);

                    messageInStorage[message.ClassId].Body = Encoding.UTF8.GetBytes(xDocument.ToString());
                    messageWait.Add(message.Id, storageMessage.Id);
                    countMessage(message.ClassId);
                }
            }
            catch (Exception ex)
            {

                CompletePeeklock(logger, messageSource, message, MessageHandlingError.UnknowError, ex.Message);
            }

        }
        //Очистка пула накопления класса по количеству сообщений.
        public void clearPoolToClassId(string classId)
        {
            Pool.Remove(classId);
        }
        //Очистка Storage Message
        public void clearMessageStorageToClassId(string classId)
        {
            messageInStorage.Remove(classId);
        }

        public void SendBatchMessage(IMessageReplyHandler messageReply, IMessageSource messageSource, bool allPackage, Message message)
        {
            if (allPackage)
            {
                while (messageInStorage.Count() > 0)
                {
                    int indexMessageStorage = messageInStorage.Count() - 1;
                    messageReply.HandleReplyMessage(messageInStorage.ToArray()[indexMessageStorage].Value);
                    while (messageWait.Where(x => x.Value == messageInStorage.ToArray()[indexMessageStorage].Value.Id).Count() > 0)
                    {
                        int indexMessageWait = (messageWait.Where(x => x.Value == messageInStorage.ToArray()[indexMessageStorage].Value.Id).Count() - 1);
                        var listMessage = messageWait.Where(x => x.Value == messageInStorage.ToArray()[indexMessageStorage].Value.Id);
                        try
                        {             
                            messageSource.CompletePeekLock(listMessage.ToArray()[indexMessageWait].Key);
                            messageWait.Remove(listMessage.ToArray()[indexMessageWait].Key);
                        }
                        catch (Exception ex)
                        {
                            messageSource.CompletePeekLock(listMessage.ToArray()[indexMessageWait].Key, MessageHandlingError.UnknowError, "Ошибка подтверждения сообщения " + listMessage.ToArray()[indexMessageWait].Key + "\nОшибка: " + ex.Message);
                        }
                    }
                    clearPoolToClassId(messageInStorage.ToArray()[indexMessageStorage].Key);
                    clearMessageStorageToClassId(messageInStorage.ToArray()[indexMessageStorage].Key);
                }
            }
            else
            {
                messageReply.HandleReplyMessage(messageInStorage[message.ClassId]);

                while(messageWait.Where(x => x.Value == messageInStorage[message.ClassId].Id).Count() > 0)
                {
                    var indexMessageWait = (messageWait.Where(x => x.Value == messageInStorage[message.ClassId].Id).Count() - 1);
                    var listMessage = messageWait.Where(x => x.Value == messageInStorage[message.ClassId].Id);
                    try
                    {      
                        messageSource.CompletePeekLock(listMessage.ToArray()[indexMessageWait].Key);
                        messageWait.Remove(listMessage.ToArray()[indexMessageWait].Key);
                    }
                    catch (Exception ex)
                    {

                        messageSource.CompletePeekLock(listMessage.ToArray()[indexMessageWait].Key, MessageHandlingError.UnknowError, "Ошибка подтверждения сообщения " + listMessage.ToArray()[indexMessageWait].Key + "\nОшибка: " + ex.Message);
                    }
                }

                clearPoolToClassId(message.ClassId);
                clearMessageStorageToClassId(message.ClassId);
            }
        }

        public void countMessage(string classId)
        {
            if (Pool.ContainsKey(classId))
            {
                Pool[classId] = Pool[classId] + 1;
            }
            else
            {
                Pool.Add(classId, 1);
            }
        }

        public void AbandonPeeklock(ILogger logger, IMessageSource messageSource, Message message, string debugMessage = "")
        {
            messageSource.AbandonPeekLock(message.Id);
            if (string.IsNullOrEmpty(debugMessage))
                return;
            logger.Debug("Сообщение возвращено в очередь: " + debugMessage);
        }

        private void CompletePeeklock(ILogger logger, IMessageSource messageSource, Message message, MessageHandlingError messageHandlingError, string errorMessage)
        {
            messageSource.CompletePeekLock(message.Id, messageHandlingError, errorMessage);
            logger.Error("Сообщение не обработано и помещено в архив: " + errorMessage);
        }

        private void CompletePeeklock(ILogger logger, IMessageSource messageSource, Message message)
        {
            messageSource.CompletePeekLock(message.Id);
            logger.Debug("Сообщение обработано");
        }

    }
}
