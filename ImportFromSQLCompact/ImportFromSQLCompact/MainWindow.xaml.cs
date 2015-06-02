using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Win32;
using System.ComponentModel;

namespace ImportFromSQLCompact
{

    /// <summary>
    /// Логика взаимодействия для MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        Thread ImportThd;
        ImportFromSQLCE ObjImportFromSQLCE;

        public MainWindow()
        {            

            InitializeComponent();

            WICalendar.DisplayDateStart = DateTime.Now.Date.AddDays(-14);
            WICalendar.DisplayDateEnd = DateTime.Now.Date;
            WICalendar.IsTodayHighlighted = false;
            WICalendar.IsHitTestVisible = false;
            WICalendar.Focusable = false;
            WICalendar.SelectionMode = CalendarSelectionMode.MultipleRange;
            
            Thread ThUpdateCalendar = new Thread(UpdateCalendar);
            ThUpdateCalendar.Start();
                                          
        }

        private void UpdateCalendar()
        {            

            Dispatcher.BeginInvoke(new ThreadStart(delegate
            {
                WICalendar.SelectedDates.Clear();
            }));

            ImportedDates ObjImportedDates = new ImportedDates();
            DateTime[] DatesArray = ObjImportedDates.GetImportedDates(DateTime.Now.Date.AddDays(-14), DateTime.Now.Date);

            Dispatcher.BeginInvoke(new ThreadStart(delegate
            {
                for (int i = 0; i < DatesArray.Length; i++)
                {
                    WICalendar.SelectedDates.Add(DatesArray[i]);
                }

                InitializeComponent();
            }));
            
        }


        private void Button_Click(object sender, RoutedEventArgs e)
        {
            if (string.IsNullOrEmpty(SQLCompactFile.Text) == true)
            {
                Dispatcher.BeginInvoke(new ThreadStart(delegate
                {
                    MessageBox.Show(this, "Укажите файл для импорта!");
                }));
                return;
            }

            if (ImportThd != null)
            {
                if (ImportThd.IsAlive == true)
                {
                    Dispatcher.BeginInvoke(new ThreadStart(delegate
                    {
                        MessageBox.Show(this, "Импорт уже запущен!");
                    }));
                    return;
                }
            }

            PrBar.IsIndeterminate = true;
            
            ObjImportFromSQLCE = new ImportFromSQLCE(SQLCompactFile.Text);

            PrBar.Value = ObjImportFromSQLCE.CurrentProgress;

            ObjImportFromSQLCE.EndOfImportEvent += EndOfImport;
            ObjImportFromSQLCE.UpdateProgressBarPercentageEvent += UpdateProgressBarPercentage;
            ImportThd = new Thread(ObjImportFromSQLCE.DoImportFromSQL);

            ImportThd.Start();

        }


        private void EndOfImport()
        {
            UpdateCalendar();

            Dispatcher.BeginInvoke(new ThreadStart(delegate
            {
                PrBar.IsIndeterminate = false;
                if (ObjImportFromSQLCE.CompletedSuccesfully != true)
                {
                    MessageBox.Show(this, String.Format("Импорт не выполнен: {0}", ObjImportFromSQLCE.ExceptionMessage));
                }
                else
                {
                    MessageBox.Show(this, "Импорт завершен!");
                }
            }));
        }


        private void UpdateProgressBarPercentage()
        {            
            Dispatcher.BeginInvoke(new ThreadStart(delegate
                {
                    if (PrBar.IsIndeterminate == true)
                    {
                        PrBar.IsIndeterminate = false;
                    }

                    PrBar.Value = ObjImportFromSQLCE.CurrentProgress;
                }));
        }
      

        private void ButtonOpenFile_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog openDialog = new OpenFileDialog();
            openDialog.Filter = "SQL CE Database (*.sdf)|*.sdf";
            if (openDialog.ShowDialog() == true)
            {
                SQLCompactFile.Text = openDialog.FileName;
            }
        }
    }
}
