using Microsoft.AspNetCore.SignalR;

namespace RabbitMQ.Hubs
{
    public class MessageHub : Hub
    {
        public async Task JoinGroup()
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, "MessageStatus");
        }
    }
}