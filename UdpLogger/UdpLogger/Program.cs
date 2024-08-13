using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Diagnostics;

class Program
{
    private static readonly string filePath = "/home/kuzu/Desktop/output2.txt";
    private static readonly int numberOfWrites = 100000; 
    private static readonly int numberOfThreads = 10;
    private static readonly int udpPort = 11000; 
    private static readonly int udpBufferSize = 65535; 

    private static readonly UdpClient udpClient = new UdpClient(udpPort);
    private static volatile bool isWritingComplete = false;

    static void Main()
    {
        
        using (StreamWriter writer = new StreamWriter(filePath, append: false, encoding: Encoding.UTF8, bufferSize: 8192))
        {
            
        }

        var stopwatch = Stopwatch.StartNew();

        
        Thread udpServerThread = new Thread(UdpServer);
        udpServerThread.Start();

        
        Thread[] workers = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++)
        {
            int threadIndex = i;
#pragma warning disable CS0612 
            workers[i] = new Thread(() => Worker(threadIndex));
            workers[i].Start();
        }

        
        foreach (var worker in workers)
        {
            worker.Join();
        }

        isWritingComplete = true;

        udpServerThread.Join();

        stopwatch.Stop();
        Console.WriteLine($"Total execution time: {stopwatch.ElapsedMilliseconds} ms");
        Console.WriteLine("All threads have finished executing. Timestamp written to file.");
    }

    [Obsolete]
    static void Worker(int threadIndex)
{
    using (UdpClient client = new UdpClient())
    {
        int messagesSent = 0;
        IPEndPoint endPoint = new IPEndPoint(IPAddress.Loopback, udpPort);
        for (int i = 0; i < numberOfWrites; i++)
        {
            long threadId = AppDomain.GetCurrentThreadId();
            string logMessage = $"Thread {threadIndex} :: {threadId} - Write {i + 1} - : {DateTime.Now}\n";
            byte[] messageBytes = Encoding.UTF8.GetBytes(logMessage);

            try
            {
                client.Send(messageBytes, messageBytes.Length, "localhost", udpPort);

                
                client.Client.ReceiveTimeout = 1000; 
                try
                {
                    byte[] ack = client.Receive(ref endPoint);
                    
                }
                catch (SocketException)
                {
                    
                }

                messagesSent++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Thread {threadIndex}: Sending failed for log message {i + 1}: {ex.Message}");
            }
        }

        Console.WriteLine($"Thread {threadIndex} completed sending {messagesSent} messages.");
    }
}

    static void UdpServer()
    {
        IPEndPoint endPoint = new IPEndPoint(IPAddress.Any, udpPort);
        int messagesReceived = 0;

        
        udpClient.Client.ReceiveBufferSize = udpBufferSize;

        using (FileStream fs = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.None))
        using (StreamWriter writer = new StreamWriter(fs, Encoding.UTF8))
        {
            while (!isWritingComplete || udpClient.Available > 0)
            {
                try
                {
                    // Check if there are any available bytes to read
                    if (udpClient.Available > 0)
                    {
                        byte[] data = udpClient.Receive(ref endPoint);
                        if (data.Length > 0)
                        {
                            string logMessage = Encoding.UTF8.GetString(data);
                            writer.Write(logMessage);
                            messagesReceived++;

                            byte[] ack = Encoding.UTF8.GetBytes("ACK");
                            udpClient.Send(ack, ack.Length, endPoint);
                        }
                    }
                    else
                    {
                        Thread.Sleep(1);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"UDP Server error: {ex.Message}");
                }
            }
        }

        Console.WriteLine($"UDP Server completed. Total messages received: {messagesReceived}");
    }
}
