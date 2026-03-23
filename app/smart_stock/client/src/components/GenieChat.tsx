import React, { useState, useEffect, useRef } from 'react';
import { Send, Bot, User, Sparkles, Database, AlertCircle, ChevronDown, ChevronUp, Plus, ExternalLink } from 'lucide-react';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  sql?: string;
  result?: any[];
  error?: string;
  status?: 'sending' | 'processing' | 'completed' | 'error';
}

interface GenieChatProps {
  onClose?: () => void;
}

export default function GenieChat({ onClose }: GenieChatProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputMessage, setInputMessage] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [showSql, setShowSql] = useState<{ [key: string]: boolean }>({});
  const [showResults, setShowResults] = useState<{ [key: string]: boolean }>({});
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    // Add welcome message
    setMessages([{
      id: 'welcome',
      role: 'assistant',
      content: 'Hi Elena! I\'m your AI assistant. I can help you analyze inventory data, check stock levels, and answer questions about your supply chain. What would you like to know?',
      timestamp: new Date(),
      status: 'completed'
    }]);
  }, []);

  const sendMessage = async () => {
    if (!inputMessage.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: inputMessage,
      timestamp: new Date(),
      status: 'completed'
    };

    setMessages(prev => [...prev, userMessage]);
    setInputMessage('');
    setIsLoading(true);

    // Add loading message
    const loadingMessage: Message = {
      id: Date.now().toString() + '-loading',
      role: 'assistant',
      content: 'Analyzing your request...',
      timestamp: new Date(),
      status: 'processing'
    };
    setMessages(prev => [...prev, loadingMessage]);

    try {
      const response = await fetch('/api/genie/send-message', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: inputMessage,
          conversation_id: conversationId
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to send message');
      }

      const data = await response.json();

      if (!conversationId) {
        setConversationId(data.conversation_id);
      }

      // Remove loading message
      setMessages(prev => prev.filter(m => m.id !== loadingMessage.id));

      // Add assistant response
      const assistantMessage: Message = {
        id: data.message_id,
        role: 'assistant',
        content: data.content || 'I processed your request.',
        timestamp: new Date(),
        sql: data.sql_query,
        result: data.query_result,
        error: data.error,
        status: 'completed'
      };

      setMessages(prev => [...prev, assistantMessage]);
      // Automatically show results for this message
      if (assistantMessage.result) {
        setShowResults(prev => ({ ...prev, [assistantMessage.id]: true }));
      }
    } catch (error) {
      // Remove loading message
      setMessages(prev => prev.filter(m => m.id !== loadingMessage.id));

      // Add error message
      const errorMessage: Message = {
        id: Date.now().toString() + '-error',
        role: 'assistant',
        content: 'I encountered an error processing your request. Please try again.',
        timestamp: new Date(),
        error: error instanceof Error ? error.message : 'Unknown error',
        status: 'error'
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      inputRef.current?.focus();
    }
  };

  const suggestedQuestions = [
    'What are my current critical stock items?',
    'Show me the top 5 products by sales this week',
    'What\'s the current inventory turnover rate?',
    'Which suppliers have the best on-time delivery?'
  ];

  const toggleSql = (messageId: string) => {
    setShowSql(prev => ({ ...prev, [messageId]: !prev[messageId] }));
  };

  const toggleResults = (messageId: string) => {
    setShowResults(prev => ({ ...prev, [messageId]: !prev[messageId] }));
  };

  const startNewChat = () => {
    setConversationId(null);
    setMessages([{
      id: 'welcome',
      role: 'assistant',
      content: 'Hi Elena! I\'m your AI assistant. I can help you analyze inventory data, check stock levels, and answer questions about your supply chain. What would you like to know?',
      timestamp: new Date(),
      status: 'completed'
    }]);
    setInputMessage('');
  };

  const openGenieSpace = () => {
    const genieSpaceId = '01f0b8993ae91634b351d60035aa7c31';
    const databricksHost = 'https://fe-vm-nam-nguyen-workspace-classic.cloud.databricks.com';
    const genieUrl = `${databricksHost}/genie/rooms/${genieSpaceId}`;
    window.open(genieUrl, '_blank');
  };

  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="flex items-center justify-between p-4 pr-24 border-b bg-gradient-to-r from-blue-600 to-blue-700">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-white/20 rounded-lg backdrop-blur">
            <Sparkles className="h-5 w-5 text-white" />
          </div>
          <div>
            <h3 className="text-white font-semibold">PharmaStock AI Assistant</h3>
            <p className="text-xs text-blue-100">Powered by Databricks Genie</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {conversationId && (
            <button
              onClick={startNewChat}
              className="flex items-center gap-1 px-3 py-1.5 bg-white/20 hover:bg-white/30 text-white rounded-lg text-sm transition-colors"
              title="Start a new chat session"
            >
              <Plus className="h-4 w-4" />
              New Chat
            </button>
          )}
          <button
            onClick={openGenieSpace}
            className="flex items-center gap-1 px-3 py-1.5 bg-white/20 hover:bg-white/30 text-white rounded-lg text-sm transition-colors"
            title="Open Genie in Databricks workspace"
          >
            <ExternalLink className="h-4 w-4" />
            Open Full Genie
          </button>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Display messages first */}
        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex gap-3 ${message.role === 'user' ? 'flex-row-reverse' : ''}`}
          >
            <div className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
              message.role === 'user' ? 'bg-blue-600' : 'bg-gray-100'
            }`}>
              {message.role === 'user' ? (
                <User className="h-4 w-4 text-white" />
              ) : (
                <Bot className="h-4 w-4 text-gray-600" />
              )}
            </div>

            <div className={`flex-1 max-w-[80%] ${message.role === 'user' ? 'text-right' : ''}`}>
              <div className={`inline-block p-3 rounded-lg ${
                message.role === 'user'
                  ? 'bg-blue-600 text-white'
                  : message.status === 'error'
                  ? 'bg-red-50 text-red-900 border border-red-200'
                  : message.status === 'processing'
                  ? 'bg-gray-50 text-gray-600'
                  : 'bg-gray-100 text-gray-900'
              }`}>
                {message.status === 'processing' && (
                  <div className="flex items-center gap-2">
                    <div className="animate-pulse">⏳</div>
                    <span className="text-sm">{message.content}</span>
                  </div>
                )}
                {message.status !== 'processing' && (
                  <p className="text-sm whitespace-pre-wrap">{message.content}</p>
                )}

                {message.sql && (
                  <div className="mt-3 pt-3 border-t border-gray-200">
                    <button
                      onClick={() => toggleSql(message.id)}
                      className="flex items-center gap-2 text-xs font-medium text-blue-600 hover:text-blue-700"
                    >
                      <Database className="h-3 w-3" />
                      {showSql[message.id] ? 'Hide' : 'Show'} SQL Query
                      {showSql[message.id] ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                    </button>
                    {showSql[message.id] && (
                      <pre className="mt-2 p-2 bg-gray-900 text-gray-100 rounded text-xs overflow-x-auto">
                        {message.sql}
                      </pre>
                    )}
                  </div>
                )}

                {message.result && (
                  <div className="mt-3 pt-3 border-t border-gray-200">
                    <button
                      onClick={() => toggleResults(message.id)}
                      className="flex items-center gap-2 text-xs font-medium text-blue-600 hover:text-blue-700"
                    >
                      <Database className="h-3 w-3" />
                      {showResults[message.id] !== false ? 'Hide' : 'Show'} Results
                      {message.result.row_count !== undefined && ` (${message.result.row_count} rows)`}
                      {showResults[message.id] !== false ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                    </button>
                    {showResults[message.id] !== false && (
                      <div className="mt-2">
                        {/* Check if results have the structured format with columns and data */}
                        {message.result.columns && message.result.data ? (
                          <div className="overflow-x-auto bg-white border border-gray-200 rounded-lg">
                            <table className="min-w-full text-xs">
                              <thead>
                                <tr className="bg-gray-50 border-b">
                                  {message.result.columns.map((col: string, idx: number) => (
                                    <th key={idx} className="px-3 py-2 text-left font-medium text-gray-700">
                                      {col}
                                      {message.result.column_types && message.result.column_types[idx] && (
                                        <span className="ml-1 text-gray-400 font-normal text-xs">
                                          ({message.result.column_types[idx]})
                                        </span>
                                      )}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody className="divide-y divide-gray-200">
                                {message.result.data.slice(0, 10).map((row: any[], rowIdx: number) => (
                                  <tr key={rowIdx} className="hover:bg-gray-50">
                                    {row.map((value: any, colIdx: number) => (
                                      <td key={colIdx} className="px-3 py-2 text-gray-700">
                                        {value !== null && value !== undefined && value !== "" ? (
                                          // Format numbers if applicable
                                          typeof value === 'number' && !Number.isInteger(value) ?
                                            value.toFixed(2) :
                                            String(value)
                                        ) : (
                                          <span className="text-gray-400 italic">null</span>
                                        )}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                            {message.result.data.length > 10 && (
                              <div className="px-3 py-2 bg-gray-50 border-t text-xs text-gray-500 text-center">
                                Showing first 10 rows of {message.result.row_count || message.result.data.length} total
                              </div>
                            )}
                            {message.result.data.length === 0 && (
                              <div className="px-3 py-4 text-xs text-gray-500 text-center">
                                No results found
                              </div>
                            )}
                          </div>
                        ) : (
                          // Fallback for any other format
                          <div className="p-3 bg-gray-50 rounded-lg text-xs text-gray-600">
                            <pre className="overflow-x-auto">{JSON.stringify(message.result, null, 2)}</pre>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {message.error && (
                  <div className="mt-2 p-2 bg-red-50 rounded flex items-start gap-2">
                    <AlertCircle className="h-4 w-4 text-red-600 flex-shrink-0 mt-0.5" />
                    <p className="text-xs text-red-700">{message.error}</p>
                  </div>
                )}
              </div>

              <p className="text-xs text-gray-500 mt-1">
                {message.timestamp.toLocaleTimeString()}
              </p>
            </div>
          </div>
        ))}

        {/* Show suggestions after the greeting message */}
        {messages.length === 1 && (
          <div className="mt-4">
            <p className="text-sm text-gray-600 mb-3">Try asking:</p>
            <div className="space-y-2">
              {suggestedQuestions.map((question, index) => (
                <button
                  key={index}
                  onClick={() => {
                    setInputMessage(question);
                    inputRef.current?.focus();
                  }}
                  className="block w-full text-left p-3 bg-blue-50 hover:bg-blue-100 rounded-lg text-sm text-blue-700 transition-colors"
                >
                  {question}
                </button>
              ))}
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="border-t p-4">
        <div className="flex gap-2">
          <input
            ref={inputRef}
            type="text"
            value={inputMessage}
            onChange={(e) => setInputMessage(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
            placeholder="Ask about your inventory data..."
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
            disabled={isLoading}
          />
          <button
            onClick={sendMessage}
            disabled={isLoading || !inputMessage.trim()}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {isLoading ? (
              <div className="animate-spin h-5 w-5 border-2 border-white border-t-transparent rounded-full" />
            ) : (
              <Send className="h-5 w-5" />
            )}
          </button>
        </div>
        <p className="text-xs text-gray-500 mt-2">
          Powered by Databricks Genie • Natural language to SQL
        </p>
      </div>
    </div>
  );
}