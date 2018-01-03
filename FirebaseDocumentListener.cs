using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Firestore.V1Beta1;
using Google.Protobuf;
using Grpc.Core;
using static Google.Cloud.Firestore.V1Beta1.Target.Types;
using static Google.Cloud.Firestore.V1Beta1.TargetChange.Types;

/*
 *  An example document listener for firebase
 *  
 *  Firebase will terminate our listen request every 5 minutes, so we need a system to resend our requests so they pick up
 *  where they last left off. To accomplish this, I use a BlockingCollection, a status flags and some threads. 
 *
 */
namespace Google.Cloud.Firestore
{
    public class FirebaseDocumentListener
    {
        private bool Done = false;
        private FirestoreClient.ListenStream DuplexStream;
        private FirestoreClient FirestoreClient;
        private CallSettings ListenSettings;
        public string ProjectId { get; set; }
        public string DatabaseId { get; set; }
        private bool ListenerIsActive = false;
        private BlockingCollection<ListenRequest> PendingRequests = new BlockingCollection<ListenRequest>();
        private Queue<ListenRequest> ActiveRequests = new Queue<ListenRequest>();
        private Task RequestHanderTask = null;
        private Task ResponseHanderTask = null;
        private CancellationTokenSource CancellationTokenSource;
        private CancellationToken CancellationToken;
        private FirestoreDb FirestoreDb { get; set; }

        public FirebaseDocumentListener(FirestoreDb db)
        {
            // Create client
            FirestoreDb = db;
            FirestoreClient = db.Client; //FirestoreClient.Create();
            ProjectId = db.ProjectId;
            DatabaseId = db.DatabaseId;

            //Setup no expiration for the listen
            ListenSettings = CallSettings.FromCallTiming(CallTiming.FromExpiration(Expiration.None));

            //Start our handler for writing requests to GCP
            RequestHanderTask = StartRequestHandlerTask();

            //Initialize a cancelation source so we can cancel tasks we create
            CancellationTokenSource = new CancellationTokenSource();
            CancellationToken = CancellationTokenSource.Token;
        }

        //
        // Cancel()
        //
        // Stop the listener
        public void Cancel()
        {
            Done = true;
            this.CancellationTokenSource.Cancel();
        }

        //
        // ListenToDocument(string documentPath)
        //
        // Listen to changes in a specific document
        public void ListenToDocument(string documentPath)
        {
            // Initialize a request
            var dt = new DocumentsTarget { };
            dt.Documents.Add(documentPath);

            ListenRequest request = new ListenRequest
            {
                Database = new DatabaseRootName(ProjectId, DatabaseId).ToString(),
                AddTarget = new Target
                {
                    Documents = dt
                }
            };
            PendingRequests.Add(request);
        }

        //
        // ListenToQuery(StructuredQuery query)
        //
        // Start listening to a query. I've not tried listening to multiple quries, your mileage may vary with that.
        public void ListenToQuery(StructuredQuery query)
        {
            var qt = new QueryTarget { };
            qt.StructuredQuery = query;
            qt.Parent = string.Format("projects/{0}/databases/{1}/documents", ProjectId, DatabaseId);

            ListenRequest request = new ListenRequest
            {
                Database = new DatabaseRootName(ProjectId, DatabaseId).ToString(),
                AddTarget = new Target
                {
                    Query = qt,
                }
            };
            PendingRequests.Add(request);
        }

        //
        // Various Events raised from messages on the response stream
        //
        public event ErrorEventHandler Error;
        public event DebugMessageEventHandler DebugMessage;
        public event EventHandler Reset;
        public event EventHandler Current;
        public event DocumentEventHandler DocumentChanged;
        public event DocumentIdEventHandler DocumentRemoved;
        public event DocumentIdEventHandler DocumentDeleted;
        public event DocumentCountEventHandler DocumentFiltered;

        public class DocumentEventArgs : EventArgs
        {
            public Document Document { get; set; }
            public DocumentSnapshot DocumentSnapshot { get; set; }
        }
        public delegate void DocumentEventHandler(object sender, DocumentEventArgs e);

        public class DocumentIdEventArgs : EventArgs
        {
            public string Id { get; set; }
        }
        public delegate void DocumentIdEventHandler(object sender, DocumentIdEventArgs e);


        public class DocumentCountEventArgs : EventArgs
        {
            public int Count { get; set; }
        }
        public delegate void DocumentCountEventHandler(object sender, DocumentCountEventArgs e);

        public class MessageEventArgs : EventArgs
        {
            public string Message { get; set; }
        }
        public delegate void ErrorEventHandler(object sender, MessageEventArgs e);

        private void OnError(string message)
        {
            if (Error != null) Error(this, new MessageEventArgs { Message = message });
        }

        public delegate void DebugMessageEventHandler(object sender, MessageEventArgs e);
        private void OnDebugMessage(string message)
        {
            if (DebugMessage != null) DebugMessage(this, new MessageEventArgs { Message = message });
        }

        //
        // IsPermanentError(RpcException exception)
        //
        // Determins if the error is recoverable or not.
        // Borrowed from https://github.com/googleapis/nodejs-firestore/blob/ed83393ac9f646e33f429485a8e0ddcdd77ecb84/src/watch.js
        private bool IsPermanentError(RpcException exception)
        {
            if (exception == null) return false;
            switch (exception.Status.StatusCode)
            {
                case StatusCode.Cancelled:
                case StatusCode.Unknown:
                case StatusCode.DeadlineExceeded:
                case StatusCode.ResourceExhausted:
                case StatusCode.Internal:
                case StatusCode.Unavailable:
                case StatusCode.Unauthenticated:
                    return false;
                default:
                    return true;
            }
        }

        //
        // StartRequestHandlerTask()
        //
        // Take any messages on our PendingRequest list and write them to GCP
        private Task StartRequestHandlerTask()
        {
            OnDebugMessage("Started Request Handler");
            return Task.Run(async () =>
            {
                while (!Done)
                {
                    var request = PendingRequests.Take(CancellationToken);
                    OnDebugMessage("Setup listen for request");

                    //If the listener isn't active, start it
                    if (DuplexStream == null || !ListenerIsActive)
                    {
                        // Initialize streaming call, retrieving the stream object
                        DuplexStream = FirestoreClient.Listen(ListenSettings);
                        ListenerIsActive = true;
                        OnDebugMessage("Response Task Not Active, starting");
                        ResponseHanderTask = StartResponseHandlerTask();
                    }
                    OnDebugMessage("Sending Request");
                    // Stream a request to the server
                    await DuplexStream.WriteAsync(request);
                    ActiveRequests.Enqueue(request);
                }
                OnDebugMessage("Request Handler Completed");
                await DuplexStream.WriteCompleteAsync();
            });
        }

        //
        // RestartAllRequests()
        //
        // GCP closes the stream every 5 minutes. Re-request whatever we are listening for 
        private void RestartAllRequests()
        {
            OnDebugMessage("Restarting Requests");
            while (ActiveRequests.Count > 0)
            {
                PendingRequests.Add(ActiveRequests.Dequeue());
            }
        }

        //
        // CreateDocumentSnapshot(DocumentChange documentChange)
        //
        // Generate a DocumentSnapshot from a Document
        private DocumentSnapshot CreateDocumentSnapshot(Document document)
        {
            //Creation of DocumentSnapshots is internal so we are very much cheating and using reflection to access them
            var snapshotType = typeof(DocumentSnapshot);
            var snapshotMethods = snapshotType.GetMethods(BindingFlags.Static | BindingFlags.NonPublic);
            var forDocument = snapshotMethods.FirstOrDefault(m => m.Name == "ForDocument");
            //It's probably bad to use the current timestamp, but we don't seem to have access to the readTime in ListenResponse
            DocumentSnapshot snapshot = null;
            try
            {
                snapshot = forDocument.Invoke(null, new object[] { FirestoreDb, document, Timestamp.GetCurrentTimestamp() }) as DocumentSnapshot;
            }
            catch (Exception ex)
            {
                OnError(ex.ToString());
            }
            return snapshot;
        }

        //
        // StartResponseHandlerTask()
        //
        // Read responses from the response stream and dispatch events as necessary
        private Task StartResponseHandlerTask()
        {
            OnDebugMessage("Starting Response Handler");
            return Task.Run(async () =>
            {
                IAsyncEnumerator<ListenResponse> responseStream = DuplexStream.ResponseStream;
                try
                {
                    //ListenerIsActive = true;
                    while (await responseStream.MoveNext(CancellationToken))
                    {
                        ListenResponse response = responseStream.Current;
                        if (response.TargetChange != null)
                        {
                            if (response.TargetChange.TargetChangeType == TargetChangeType.NoChange)
                            {
                                if (response.TargetChange.ResumeToken != null && response.TargetChange.ResumeToken.Length > 0)
                                {
                                    foreach (var request in ActiveRequests)
                                    {
                                        request.AddTarget.ResumeToken = response.TargetChange.ResumeToken;
                                    }
                                }
                            }
                            else if (response.TargetChange.TargetChangeType == TargetChangeType.Add)
                            {
                                //Not much to be done here, if we follow the Node.js example we could set a TargetId when 
                                //we create the request, then ensure it is returned                            
                            }
                            else if (response.TargetChange.TargetChangeType == TargetChangeType.Remove)
                            {
                                //Remove called, shutdown the listener
                                OnError("Document Removed - " + response.TargetChange.Cause.Message);
                                this.Cancel();
                            }
                            else if (response.TargetChange.TargetChangeType == TargetChangeType.Reset)
                            {
                                if (Reset != null) Reset(this, new EventArgs());
                            }
                            else if (response.TargetChange.TargetChangeType == TargetChangeType.Current)
                            {
                                if (Current != null) Current(this, new EventArgs());
                            }
                            else
                            {
                                OnError("Unknown TargetChangeType");
                            }
                        }
                        else if (response.DocumentChange != null)
                        {
                            var snapshot = CreateDocumentSnapshot(response.DocumentChange.Document);
                            if (DocumentChanged != null) DocumentChanged(this, new DocumentEventArgs { Document = response.DocumentChange.Document, DocumentSnapshot = snapshot });
                        }
                        else if (response.DocumentRemove != null)
                        {
                            if (DocumentRemoved != null) DocumentRemoved(this, new DocumentIdEventArgs { Id = response.DocumentRemove.Document });
                        }
                        else if (response.DocumentDelete != null)
                        {
                            if (DocumentDeleted != null) DocumentDeleted(this, new DocumentIdEventArgs { Id = response.DocumentDelete.Document });
                        }
                        else if (response.Filter != null)
                        {
                            if (DocumentFiltered != null) DocumentFiltered(this, new DocumentCountEventArgs { Count = response.Filter.Count });
                        }
                        else
                        {
                            OnError("Unknown listen response type");
                            Cancel();
                        }
                    }
                }
                catch (RpcException ex)
                {
                    ListenerIsActive = false;

                    var status = DuplexStream.GrpcCall.GetStatus();
                    OnDebugMessage(string.Format("Handling Exception: stream status {0} - {1}", status.StatusCode.ToString(), status.Detail));

                    if (CancellationToken.IsCancellationRequested)
                    {
                        OnDebugMessage("Cancel Requested - will not attempt recovery.");
                    }
                    else if (!IsPermanentError(ex))
                    {
                        OnDebugMessage("Attempting to recover");
                        RestartAllRequests();
                    }
                }
                OnDebugMessage("Response Handler Completed");
            });
        }

    }
}