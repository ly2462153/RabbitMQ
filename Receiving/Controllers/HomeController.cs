using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Receiving.Models;
using RabbitMQ.Client;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;

namespace Receiving.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IHttpClientFactory _httpClientFactory;

        public HomeController(ILogger<HomeController> logger, IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _httpClientFactory = httpClientFactory;
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ReceiveMessageFromQueue1()
        {
            string receivedMessage = "No new messages";
            try
            {
                var factory = new ConnectionFactory { HostName = "localhost" };
                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                // 1. 声明Direct交换机（与发送端队列1保持一致）
                await channel.ExchangeDeclareAsync(
                    exchange: "hello_exchange1",
                    type: "direct",
                    durable: false,
                    autoDelete: false
                );

                // 2. 声明队列
                await channel.QueueDeclareAsync(
                    queue: "hello1",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                // 3. 将队列绑定到交换机（与发送端队列1保持一致）
                await channel.QueueBindAsync(
                    queue: "hello1",
                    exchange: "hello_exchange1",
                    routingKey: "hello_key1"
                );

                // 4. 从队列1获取消息
                var result = await channel.BasicGetAsync("hello1", autoAck: true);
                if (result != null)
                {
                    var body = result.Body.ToArray();
                    receivedMessage = Encoding.UTF8.GetString(body);
                    _logger.LogInformation($"Received message from Queue1: {receivedMessage}");

                    // Notify sender that the message has been consumed
                    try
                    {
                        var client = _httpClientFactory.CreateClient();
                        var content = new StringContent(JsonSerializer.Serialize(receivedMessage), Encoding.UTF8, "application/json");
                        await client.PostAsync("https://localhost:44393/api/MessageStatus", content);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to notify sender");
                    }
                }
                else
                {
                    _logger.LogInformation("Queue1 is empty, no new messages.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while retrieving message from RabbitMQ Queue1.");
                receivedMessage = "Error while retrieving message";
            }

            return Json(new { message = receivedMessage });
        }

        [HttpPost]
        public async Task<IActionResult> ReceiveMessageFromQueue2()
        {
            string receivedMessage = "No new messages";
            try
            {
                var factory = new ConnectionFactory { HostName = "localhost" };
                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                // 1. 声明Direct交换机（与发送端队列2保持一致）
                await channel.ExchangeDeclareAsync(
                    exchange: "hello_exchange2",
                    type: "direct",
                    durable: false,
                    autoDelete: false
                );

                // 2. 声明队列
                await channel.QueueDeclareAsync(
                    queue: "hello2",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                // 3. 将队列绑定到交换机（与发送端队列2保持一致）
                await channel.QueueBindAsync(
                    queue: "hello2",
                    exchange: "hello_exchange2",
                    routingKey: "hello_key2"
                );

                // 4. 从队列2获取消息
                var result = await channel.BasicGetAsync("hello2", autoAck: true);
                if (result != null)
                {
                    var body = result.Body.ToArray();
                    receivedMessage = Encoding.UTF8.GetString(body);
                    _logger.LogInformation($"Received message from Queue2: {receivedMessage}");

                    // Notify sender that the message has been consumed
                    try
                    {
                        var client = _httpClientFactory.CreateClient();
                        var content = new StringContent(JsonSerializer.Serialize(receivedMessage), Encoding.UTF8, "application/json");
                        await client.PostAsync("https://localhost:44393/api/MessageStatus", content);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to notify sender");
                    }
                }
                else
                {
                    _logger.LogInformation("Queue2 is empty, no new messages.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while retrieving message from RabbitMQ Queue2.");
                receivedMessage = "Error while retrieving message";
            }

            return Json(new { message = receivedMessage });
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