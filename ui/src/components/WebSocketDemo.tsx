import { useStateStore } from '@/hooks/use-state';
import { useState, useCallback, useEffect } from 'react';
import useWebSocket, { ReadyState } from 'react-use-websocket';

interface Message {
  updateType: string
  data: TaskUpdate[]
}

export const WebSocketDemo = () => {
  //Public API that will echo messages sent to it back to the client
  const updateTask = useStateStore(state => state.updateTask)
  // console.log(state?.tasks.tasks)
  const [socketUrl, setSocketUrl] = useState('ws://localhost:8000/ws');
  const [messageHistory, setMessageHistory] = useState<MessageEvent<any>[]>([]);

  const { sendMessage, lastJsonMessage, readyState } = useWebSocket<Message>(socketUrl, {
    shouldReconnect: (closeEvent) => true,
  });

  useEffect(() => {
    if (lastJsonMessage !== null) {
      // setMessageHistory((prev) => prev.concat(lastMessage));
      console.log("lastJsonMessage", lastJsonMessage)
      if (lastJsonMessage.updateType === 'code_change') {
        updateTask(lastJsonMessage.data)
      }
    }
  }, [lastJsonMessage]);

  const handleClickSendMessage = useCallback(() => sendMessage('Hello'), []);

  const connectionStatus = {
    [ReadyState.CONNECTING]: 'Connecting',
    [ReadyState.OPEN]: 'Open',
    [ReadyState.CLOSING]: 'Closing',
    [ReadyState.CLOSED]: 'Closed',
    [ReadyState.UNINSTANTIATED]: 'Uninstantiated',
  }[readyState];

  return (null
    // <span>WebSocket: {connectionStatus}</span>
    // <div>
    //   {/* <button
    //     onClick={handleClickSendMessage}
    //     disabled={readyState !== ReadyState.OPEN}
    //   >
    //     Click Me to send 'Hello'
    //   </button> */}
      // {/* {lastMessage ? <span>Last message: {lastMessage.data}</span> : null}
      // <ul>
      //   {messageHistory.map((message, idx) => (
      //     <span key={idx}>{message ? message.data : null}</span>
      //   ))}
      // </ul> */}
    // </div>
  );
};