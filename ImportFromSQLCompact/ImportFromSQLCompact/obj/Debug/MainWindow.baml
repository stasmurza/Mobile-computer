   M S B A M L   `   `   `  ����O  KImportFromSQLCompact, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null;"clr-namespace:ImportFromSQLCompactImportFromSQLCompact  5   	   %    ImportFromSQLCompact.MainWindow   5   	   R NWindowsBase, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35W SPresentationCore, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35\ XPresentationFramework, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35D 9http://schemas.microsoft.com/winfx/2006/xaml/presentation    5   	   R NSystem.Xaml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089:x,http://schemas.microsoft.com/winfx/2006/xaml     5   	   0p"clr-namespace:ImportFromSQLCompact     5   	     � Title$  Import from SQL Compact��5   	    � SizeToContent$ WidthAndHeight=�6)   $	��100��6H   .��5      � .+�5   
   �� .+�5      ��  �� Orientation$ 
Horizontal=�6   .+�5	      �� $
��Left=�6   $��5,5,5,5q�67   $
��Wrap=�6H   $��SQL Compact file:��6\   $	��Top=�6u   �� 5
      -   5        $��SQLCompactFile��5
      $   ImportFromSQLCompact.Resources   DefaultSQLCompactFile#��Z 60    �� 
IsReadOnly . $
��Left=�6v   $��5,5,5,5q�6�    �� TextWrapping$
 Wrap=�6�   $	��Top=�6�   �� 5      -   5        $��ButtonOpenFile��5      $
��Open��6/   $��Right=�6>   $��5,5,5,5q�6Z   $	��Top=�6k   6�   5      �� 5      $��Bottom=�6   .+�5      � -   5        $��PrBar��5      $��
5, 5, 5, 5q�6+   $��10��6?   6P   �� 5      -   5        $��Import��5      $��
5, 5, 5, 5q�6+   6?   5      &   System.Windows.Controls.Calendar  5      -   5        $��
WICalendar��5      
���   �6?   5      5      5      