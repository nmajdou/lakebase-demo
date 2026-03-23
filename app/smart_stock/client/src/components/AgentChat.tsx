import React, { useState, useEffect, useRef } from 'react';
import { Send, Bot, User, Sparkles, AlertCircle } from 'lucide-react';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  status?: 'sending' | 'processing' | 'completed' | 'error';
  error?: string;
}

interface SuggestedAction {
  label: string;
  message: string;
  icon: string;
}

// Demo flow detection - returns suggested action buttons based on conversation context
const getDemoFlowSuggestions = (messages: Message[]): SuggestedAction[] => {
  if (messages.length < 2) return [];
  
  // Find the last user message
  const lastUserMessage = [...messages].reverse().find(m => m.role === 'user');
  const lastAssistantMessage = [...messages].reverse().find(m => m.role === 'assistant' && m.status === 'completed');
  
  if (!lastUserMessage || !lastAssistantMessage) return [];
  
  const userContent = lastUserMessage.content.toLowerCase();
  const assistantContent = lastAssistantMessage.content.toLowerCase();
  
  // Stage 1: After asking about inventory issues/alerts
  const isAskingAboutInventory = 
    userContent.includes("what's wrong") || 
    userContent.includes("whats wrong") ||
    userContent.includes("inventory") && (userContent.includes("issue") || userContent.includes("alert") || userContent.includes("problem") || userContent.includes("wrong")) ||
    userContent.includes("stock") && (userContent.includes("issue") || userContent.includes("alert") || userContent.includes("problem"));
  const hasAlertData = 
    assistantContent.includes("urgent") || 
    assistantContent.includes("alert") || 
    assistantContent.includes("stockout") || 
    assistantContent.includes("risk") ||
    assistantContent.includes("days until") ||
    assistantContent.includes("reorder point") ||
    assistantContent.includes("low stock");
  
  if (isAskingAboutInventory && hasAlertData) {
    return [
      { icon: '💰', label: "What's the financial impact?", message: "What's the financial impact of the stockout?" },
      { icon: '📦', label: 'Show reorder options', message: 'What are my options for reordering?' },
    ];
  }
  
  // Stage 2: After asking about financial impact
  const isAskingAboutImpact = 
    userContent.includes("financial impact") || 
    userContent.includes("financial") && userContent.includes("impact") ||
    userContent.includes("stockout") && userContent.includes("impact") ||
    userContent.includes("cost") && userContent.includes("stockout");
  const hasFinancialData = 
    assistantContent.includes("€") || 
    assistantContent.includes("eur") || 
    assistantContent.includes("revenue") || 
    assistantContent.includes("cost") ||
    assistantContent.includes("impact") ||
    assistantContent.includes("exposure");
  
  if (isAskingAboutImpact && hasFinancialData) {
    return [
      { icon: '📦', label: 'Show reorder options', message: 'What are my options for reordering?' },
    ];
  }
  
  // Stage 3: After asking about reorder options
  // Check for various ways the agent might present options
  const isAskingAboutOptions = userContent.includes("option") && (userContent.includes("reorder") || userContent.includes("order"));
  const hasSupplierOptions = 
    (assistantContent.includes("option a") || assistantContent.includes("option b") || assistantContent.includes("option c")) ||
    (assistantContent.includes("eurobike") || assistantContent.includes("velotech") || assistantContent.includes("cyclecore")) ||
    (assistantContent.includes("fastest") && assistantContent.includes("economical")) ||
    (assistantContent.includes("supplier") && assistantContent.includes("total"));
  
  if (isAskingAboutOptions && hasSupplierOptions) {
    return [
      { icon: '⚡', label: 'Proceed with Option A (Fastest)', message: 'Please proceed with Option A' },
      { icon: '⚖️', label: 'Proceed with Option B (Balanced)', message: 'Please proceed with Option B' },
      { icon: '💵', label: 'Proceed with Option C (Economical)', message: 'Please proceed with Option C' },
    ];
  }
  
  // Stage 4: After order completion
  if (
    (userContent.includes("proceed") && userContent.includes("option")) &&
    (assistantContent.includes("po-") || assistantContent.includes("order") && assistantContent.includes("created"))
  ) {
    return [
      { icon: '🔍', label: 'Check for more issues', message: "Hello PharmaStock AI, what's wrong with my inventory?" },
    ];
  }
  
  return [];
}

interface AgentChatProps {
  onClose?: () => void;
  initialMessage?: string;
  onMessageSent?: () => void;
  onNewChat?: () => void;
  showNewChatButton?: boolean;
}

export default function AgentChat({ onClose, initialMessage, onMessageSent, onNewChat, showNewChatButton = true }: AgentChatProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputMessage, setInputMessage] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [showActionButtons, setShowActionButtons] = useState(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const hasProcessedInitialMessage = useRef(false);

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
      content: 'Hi! I\'m your PharmaStock AI Assistant. I can help you analyze inventory data, check stock levels, forecast demand, and answer questions about your supply chain. What would you like to know?',
      timestamp: new Date(),
      status: 'completed'
    }]);
  }, []);

  // Handle initial message auto-send
  useEffect(() => {
    if (initialMessage && !hasProcessedInitialMessage.current && messages.length > 0) {
      hasProcessedInitialMessage.current = true;
      setInputMessage(initialMessage);
      // Wait a moment for the UI to render, then send
      const timer = setTimeout(() => {
        sendMessageWithContent(initialMessage);
        onMessageSent?.();
      }, 500);
      return () => clearTimeout(timer);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialMessage, messages.length, onMessageSent]);

  const sendMessageWithContent = async (messageContent: string) => {
    if (!messageContent.trim() || isLoading) return;

    // Hide action buttons when user sends any message
    setShowActionButtons(false);

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: messageContent,
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
      // Prepare conversation history (last 10 messages for context)
      const conversationHistory = messages
        .filter(m => m.role !== 'assistant' || m.status === 'completed')
        .slice(-10)
        .map(m => ({
          role: m.role,
          content: m.content
        }));
      
      // Add the current user message
      conversationHistory.push({
        role: 'user',
        content: messageContent
      });

      const response = await fetch('/api/agent/send-message', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          messages: conversationHistory
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(errorData.detail || 'Failed to send message');
      }

      const data = await response.json();

      // Remove loading message
      setMessages(prev => prev.filter(m => m.id !== loadingMessage.id));

      // Add assistant response
      const assistantMessage: Message = {
        id: data.message_id,
        role: 'assistant',
        content: data.content || 'I processed your request.',
        timestamp: new Date(),
        error: data.error,
        status: 'completed'
      };

      setMessages(prev => [...prev, assistantMessage]);
      
      // Show action buttons again after assistant responds
      setShowActionButtons(true);
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

  const sendMessage = () => {
    sendMessageWithContent(inputMessage);
  };

  const suggestedQuestions = [
    'What are my top 5 products by revenue?',
    'Show me current critical stock items',
    'What is the inventory turnover rate?',
    'Which products need restocking in the next 30 days?'
  ];

  const startNewChat = () => {
    setMessages([{
      id: 'welcome',
      role: 'assistant',
      content: 'Hi! I\'m your PharmaStock AI Assistant. I can help you analyze inventory data, check stock levels, forecast demand, and answer questions about your supply chain. What would you like to know?',
      timestamp: new Date(),
      status: 'completed'
    }]);
    setInputMessage('');
  };

  // Function to render markdown-like content with improved support
  const renderContent = (content: string) => {
    // Normalize content: handle both actual newlines and escaped \n
    const normalizedContent = content.replace(/\\n/g, '\n');
    const lines = normalizedContent.split('\n');
    const elements: JSX.Element[] = [];
    let inTable = false;
    let tableRows: string[] = [];
    let inList = false;
    let listItems: string[] = [];

    const flushList = (index: number) => {
      if (listItems.length > 0) {
        elements.push(
          <ul key={`list-${index}`} className="list-disc list-inside space-y-1 mb-3 ml-2">
            {listItems.map((item, i) => (
              <li key={i} className="text-sm">
                {renderLineWithFormatting(item)}
              </li>
            ))}
          </ul>
        );
        listItems = [];
        inList = false;
      }
    };

    lines.forEach((line, index) => {
      const trimmedLine = line.trim();
      
      // Skip empty lines but preserve spacing
      if (!trimmedLine) {
        flushList(index);
        if (inTable && tableRows.length > 0) {
          elements.push(renderTable(tableRows, `table-${index}`));
          tableRows = [];
          inTable = false;
        }
        return;
      }

      // Check if line is part of a table
      if (trimmedLine.startsWith('|')) {
        flushList(index);
        if (!inTable) {
          inTable = true;
          tableRows = [];
        }
        tableRows.push(line);
      } 
      // Check for headers (### Header)
      else if (trimmedLine.startsWith('###')) {
        flushList(index);
        if (inTable && tableRows.length > 0) {
          elements.push(renderTable(tableRows, `table-${index}`));
          tableRows = [];
          inTable = false;
        }
        const headerText = trimmedLine.replace(/^###\s*/, '');
        elements.push(
          <h3 key={`header3-${index}`} className="text-lg font-bold mt-4 mb-2 text-gray-900">
            {renderLineWithFormatting(headerText)}
          </h3>
        );
      }
      // Check for headers (## Header)
      else if (trimmedLine.startsWith('##')) {
        flushList(index);
        if (inTable && tableRows.length > 0) {
          elements.push(renderTable(tableRows, `table-${index}`));
          tableRows = [];
          inTable = false;
        }
        const headerText = trimmedLine.replace(/^##\s*/, '');
        elements.push(
          <h2 key={`header2-${index}`} className="text-xl font-bold mt-5 mb-3 text-gray-900">
            {renderLineWithFormatting(headerText)}
          </h2>
        );
      }
      // Check for bullet points (- item or • item)
      else if (trimmedLine.match(/^[-•]\s+/)) {
        if (inTable && tableRows.length > 0) {
          elements.push(renderTable(tableRows, `table-${index}`));
          tableRows = [];
          inTable = false;
        }
        inList = true;
        const itemText = trimmedLine.replace(/^[-•]\s+/, '');
        listItems.push(itemText);
      }
      // Check for numbered lists (1. item or 1) item)
      else if (trimmedLine.match(/^\d+[\.)]\s+/)) {
        flushList(index);
        if (inTable && tableRows.length > 0) {
          elements.push(renderTable(tableRows, `table-${index}`));
          tableRows = [];
          inTable = false;
        }
        const itemText = trimmedLine.replace(/^\d+[\.)]\s+/, '');
        elements.push(
          <div key={`numbered-${index}`} className="flex gap-2 mb-2">
            <span className="font-semibold text-gray-700 flex-shrink-0">
              {trimmedLine.match(/^\d+/)?.[0]}.
            </span>
            <span className="text-sm">
              {renderLineWithFormatting(itemText)}
            </span>
          </div>
        );
      }
      // Regular paragraph
      else {
        flushList(index);
        if (inTable && tableRows.length > 0) {
          elements.push(renderTable(tableRows, `table-${index}`));
          tableRows = [];
          inTable = false;
        }
        elements.push(
          <p key={`line-${index}`} className="mb-2 text-sm leading-relaxed">
            {renderLineWithFormatting(trimmedLine)}
          </p>
        );
      }
    });

    // Handle any remaining table or list
    if (inTable && tableRows.length > 0) {
      elements.push(renderTable(tableRows, `table-end`));
    }
    flushList(lines.length);

    return <div className="space-y-1 overflow-hidden">{elements}</div>;
  };

  const renderLineWithFormatting = (line: string) => {
    // Enhanced text formatting: bold and inline code
    const parts = line.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
    return parts.map((part, i) => {
      // Bold text
      if (part.startsWith('**') && part.endsWith('**')) {
        return <strong key={i} className="font-bold text-gray-900">{part.slice(2, -2)}</strong>;
      }
      // Inline code
      if (part.startsWith('`') && part.endsWith('`')) {
        return <code key={i} className="bg-gray-100 px-1 py-0.5 rounded text-xs font-mono">{part.slice(1, -1)}</code>;
      }
      return <span key={i}>{part}</span>;
    });
  };

  const renderTable = (rows: string[], key: string) => {
    const processedRows = rows
      .filter(row => {
        const trimmed = row.trim();
        // Remove separator rows (containing --- or ━━━ or ═══)
        return trimmed && !trimmed.match(/^\|[\s\-━═:]+\|/);
      })
      .map(row => 
        row
          .split('|')
          .map(cell => cell.trim())
          .filter(cell => cell) // Remove empty cells from start/end
      );

    if (processedRows.length === 0) return null;

    const headers = processedRows[0];
    const dataRows = processedRows.slice(1);

    return (
      <div key={key} className="my-4 border border-gray-300 rounded-lg shadow-sm relative">
        {/* Scroll container with its own horizontal scroll */}
        <div className="overflow-x-auto max-w-full scrollbar-thin scrollbar-thumb-gray-300 scrollbar-track-gray-100">
          <table className="min-w-full text-xs divide-y divide-gray-200">
            <thead className="bg-gradient-to-r from-gray-50 to-gray-100 sticky top-0">
              <tr>
                {headers.map((header, i) => (
                  <th key={i} className="px-4 py-3 text-left text-xs font-bold text-gray-800 uppercase tracking-wider whitespace-nowrap">
                    {renderLineWithFormatting(header)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {dataRows.map((row, rowIndex) => (
                <tr key={rowIndex} className={`hover:bg-blue-50 transition-colors ${rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}`}>
                  {row.map((cell, cellIndex) => (
                    <td key={cellIndex} className="px-4 py-3 text-gray-700 whitespace-nowrap">
                      {renderLineWithFormatting(cell)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {/* Scroll hint indicator */}
        <div className="absolute right-0 top-0 bottom-0 w-4 bg-gradient-to-l from-gray-200/50 to-transparent pointer-events-none rounded-r-lg" />
      </div>
    );
  };

  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="flex items-center justify-between p-5 border-b bg-gradient-to-r from-blue-600 via-indigo-600 to-purple-600 shadow-lg">
        <div className="flex items-center gap-3">
          <div className="p-2.5 bg-white/20 rounded-xl backdrop-blur">
            <Bot className="h-6 w-6 text-white" />
          </div>
          <div>
            <h3 className="text-white font-bold text-lg">PharmaStock AI Assistant</h3>
            <p className="text-xs text-blue-100 flex items-center gap-1">
              <span className="inline-block w-2 h-2 bg-green-400 rounded-full animate-pulse"></span>
              Powered by Databricks + Genie
            </p>
          </div>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
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

            <div className={`flex-1 max-w-[85%] min-w-0 ${message.role === 'user' ? 'text-right' : ''}`}>
              <div className={`p-3 rounded-lg ${
                message.role === 'user'
                  ? 'bg-blue-600 text-white inline-block'
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
                  <div className="text-sm overflow-hidden">
                    {message.role === 'assistant' && message.status !== 'error' ? (
                      renderContent(message.content)
                    ) : (
                      <p className="whitespace-pre-wrap">{message.content}</p>
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

        {/* Demo Flow Action Buttons - shown after assistant responses */}
        {showActionButtons && !isLoading && messages.length > 1 && (() => {
          const suggestions = getDemoFlowSuggestions(messages);
          if (suggestions.length === 0) return null;
          
          return (
            <div className="mt-4 mb-2">
              <p className="text-sm text-gray-500 mb-3 flex items-center gap-2">
                <span className="inline-block w-1.5 h-1.5 bg-blue-500 rounded-full"></span>
                Quick actions:
              </p>
              <div className="flex flex-wrap gap-2">
                {suggestions.map((action, index) => (
                  <button
                    key={index}
                    onClick={() => sendMessageWithContent(action.message)}
                    className="inline-flex items-center gap-2 px-4 py-2.5 bg-blue-50 hover:bg-blue-100 border border-blue-200 hover:border-blue-300 rounded-lg text-sm text-blue-700 font-medium transition-all duration-200 hover:shadow-sm"
                  >
                    <span>{action.icon}</span>
                    <span>{action.label}</span>
                  </button>
                ))}
              </div>
            </div>
          );
        })()}

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
          Powered by Databricks Foundation Models + Genie
        </p>
      </div>
    </div>
  );
}

