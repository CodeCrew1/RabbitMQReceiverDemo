using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

internal class Program
{
    private static void Main(string[] args)
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.Uri = new Uri("amqp://guest:guest@localhost:5672");
        factory.ClientProvidedName = "Rabbit receiver app 1 ";
        IConnection cnn = factory.CreateConnection();
        IModel channel = cnn.CreateModel();

        string exchangeName = "DemoExchange";
        string routingKey = "demo-routing-key";
        string queueName = "DemoQueue";

        channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
        channel.QueueDeclare(queueName, false, false, false, null);
        channel.QueueBind(queueName, exchangeName, routingKey, null);

        channel.BasicQos(0, 1, false); // basic quality of service. 
        // prefetch size - to limit the size of prefetched message. 0 means unlimited. 
        // prefetch count is 1 so we want to prefetch only one message on this receiver. so process one message at a time. so if we have multiple receiver, than other receivers will also process message instead of this single receiver prefetching all the messages and other receivers being ideal. 
        // global false as we dont want apply these setting to all receivers or just this instance of receiver. 
        
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (sender, args) =>
        {
            Task.Delay(TimeSpan.FromSeconds(3)).Wait();
            var body = args.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            Console.WriteLine($"message received is: {message}");
            channel.BasicAck(args.DeliveryTag, false);
            // once we have acknowledged message and code block execution stops, the message will be removed from the queue. 
            // if get some exception, then dont acknowledge the message. the message will be back in the queue. 
        };
        string consumerTag = channel.BasicConsume(queueName, false, consumer);
        //consume tag is tag for over all consume system.  
        Console.ReadKey();
        // so the consumer received handler keeps working and app doesnt close itself. 


        channel.BasicCancel(consumerTag);
        // we are closing the consumer with the tag after the usage of consumer with entering a key. 
        channel.Close();
        cnn.Close();
    }
}