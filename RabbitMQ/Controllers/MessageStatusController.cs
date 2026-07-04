using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Hubs;

namespace RabbitMQ.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MessageStatusController : ControllerBase
    {
        private readonly IHubContext<MessageHub> _hubContext;

        public MessageStatusController(IHubContext<MessageHub> hubContext)
        {
            _hubContext = hubContext;
        }

        [HttpPost]
        public async Task<IActionResult> UpdateStatus([FromBody] string message)
        {
            await _hubContext.Clients.Group("MessageStatus")
                .SendAsync("MessageConsumed", message);
            return Ok();
        }
    }
}
