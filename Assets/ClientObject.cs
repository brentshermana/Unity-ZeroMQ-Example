using System.Collections.Concurrent;
using System.Threading;
using NetMQ;
using UnityEngine;
using NetMQ.Sockets;

public class NetMqListener
{
    private readonly Thread _listenerWorker;

    private bool _listenerCancelled;

    public delegate void MessageDelegate(string message);

    private readonly MessageDelegate _messageDelegate;

    private readonly ConcurrentQueue<string> _messageQueue = new ConcurrentQueue<string>();

    private bool sending = true;

    private void ListenerWork()
    {
        AsyncIO.ForceDotNet.Force();
        using (var subSocket = new RequestSocket())
        {
            subSocket.Options.ReceiveHighWatermark = 1000;
            subSocket.Connect("tcp://localhost:12345");
            // subSocket.Subscribe("");
            while (!_listenerCancelled)
            {
                string frameString = "blah";
                bool success = false;

                if (!sending) {
                    success = subSocket.TryReceiveFrameString(out frameString);
                    if (success) {
                        _messageQueue.Enqueue(frameString);
                    }
                }
                else {
                    success = OutgoingSocketExtensions.TrySendFrame(subSocket, frameString);
                }

                // if the transaction was successful, 
                if (success) {
                    sending = !sending;
                }
            }
            subSocket.Close();
        }
        NetMQConfig.Cleanup();
    }

    public void Update()
    {
        while (!_messageQueue.IsEmpty)
        {
            string message;
            if (_messageQueue.TryDequeue(out message))
            {
                _messageDelegate(message);
            }
            else
            {
                break;
            }
        }
    }

    public NetMqListener(MessageDelegate messageDelegate)
    {
        _messageDelegate = messageDelegate;
        _listenerWorker = new Thread(ListenerWork);
    }

    public void Start()
    {
        _listenerCancelled = false;
        _listenerWorker.Start();
    }

    public void Stop()
    {
        _listenerCancelled = true;
        _listenerWorker.Join();
    }
}

public class ClientObject : MonoBehaviour
{
    private NetMqListener _netMqListener;

    private void HandleMessage(string message)
    {
        // TODO
    }

    private void Start()
    {
        _netMqListener = new NetMqListener(HandleMessage);
        _netMqListener.Start();
    }

    private void Update()
    {
        _netMqListener.Update();
    }

    private void OnDestroy()
    {
        _netMqListener.Stop();
    }
}
