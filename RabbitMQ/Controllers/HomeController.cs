using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Models;
using RabbitMQ.Hubs;

namespace RabbitMQ.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IHubContext<MessageHub> _hubContext;

        public HomeController(ILogger<HomeController> logger, IHubContext<MessageHub> hubContext)
        {
            _logger = logger;
            _hubContext = hubContext;
        }

        public IActionResult Index()
        {
            return View(new List<string>());
        }

        [HttpPost]
        public async Task<IActionResult> SendMessagesToQueue1()
        {
            var messages = new List<string>();

            try
            {
                var factory = new ConnectionFactory { HostName = "localhost" };
                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                // 1. 声明Direct交换机 - 队列1
                await channel.ExchangeDeclareAsync(
                    exchange: "hello_exchange1",  // 交换机名称1
                    type: "direct",
                    durable: false,
                    autoDelete: false
                );

                // 2. 声明队列 - 队列1
                await channel.QueueDeclareAsync(
                    queue: "hello1",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                // 3. 将队列绑定到交换机 - 队列1
                await channel.QueueBindAsync(
                    queue: "hello1",
                    exchange: "hello_exchange1",
                    routingKey: "hello_key1"  // 绑定键1
                );

                // 循环生成并发送5条消息到队列1
                for (int i = 1; i <= 5; i++)
                {
                    string message = $"Queue1 Message {i}";
                    var body = Encoding.UTF8.GetBytes(message);

                    // 4. 发送消息到交换机
                    await channel.BasicPublishAsync(
                        exchange: "hello_exchange1",
                        routingKey: "hello_key1",
                        body: body
                    );

                    // Add to list, initial status is pending
                    messages.Add($"{message} - Queue pending");

                    _logger.LogInformation($" [x] Sent to Queue1: {message} to hello_exchange1 with routingKey=hello_key1");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while sending messages to RabbitMQ Queue1.");
                messages.Add("Error while sending messages");
            }

            return Ok(messages);
        }

        [HttpPost]
        public async Task<IActionResult> SendMessagesToQueue2()
        {
            var messages = new List<string>();

            try
            {
                var factory = new ConnectionFactory { HostName = "localhost" };
                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                // 1. 声明Direct交换机 - 队列2
                await channel.ExchangeDeclareAsync(
                    exchange: "hello_exchange2",  // 交换机名称2
                    type: "direct",
                    durable: false,
                    autoDelete: false
                );

                // 2. 声明队列 - 队列2
                await channel.QueueDeclareAsync(
                    queue: "hello2",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                // 3. 将队列绑定到交换机 - 队列2
                await channel.QueueBindAsync(
                    queue: "hello2",
                    exchange: "hello_exchange2",
                    routingKey: "hello_key2"  // 绑定键2
                );

                // 循环生成并发送5条消息到队列2
                for (int i = 1; i <= 5; i++)
                {
                    string message = $"Queue2 Message {i}";
                    var body = Encoding.UTF8.GetBytes(message);

                    // 4. 发送消息到交换机
                    await channel.BasicPublishAsync(
                        exchange: "hello_exchange2",
                        routingKey: "hello_key2",
                        body: body
                    );

                    // Add to list, initial status is pending
                    messages.Add($"{message} - Queue pending");

                    _logger.LogInformation($" [x] Sent to Queue2: {message} to hello_exchange2 with routingKey=hello_key2");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while sending messages to RabbitMQ Queue2.");
                messages.Add("Error while sending messages");
            }

            return Ok(messages);
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}