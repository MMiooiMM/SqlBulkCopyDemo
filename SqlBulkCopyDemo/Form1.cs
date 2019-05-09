using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SqlBulkCopyDemo
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            this.Height = 320;
        }

        public int status { get; set; } = 0;

        private void button1_Click(object sender, EventArgs e)
        {
            SqlBulkCopyDemo sqlBulk = new SqlBulkCopyDemo(textBox1.Text, textBox2.Text, textBox3.Text, textBox4.Text);
            switch (status)
            {
                case 0:
                    sqlBulk.Copy();
                    break;
                case 1:
                    sqlBulk.Copy(
                        this.checkBox1.Checked, 
                        Convert.ToInt32(this.textBox5.Text), 
                        Convert.ToInt32(this.textBox6.Text)
                        );
                    break;
                case 2:
                    sqlBulk.Copy(
                       this.checkBox1.Checked,
                       Convert.ToInt32(this.textBox5.Text),
                       Convert.ToInt32(this.textBox6.Text),
                       this.textBox7.Text,
                       this.textBox8.Text
                       );
                    break;
            }            
        }

        private void button2_Click(object sender, EventArgs e)
        {
            status = 1;
            this.button2.Visible = false;
            this.checkBox1.Visible = true;
            this.label5.Visible = true;
            this.textBox5.Visible = true;
            this.label6.Visible = true;
            this.textBox6.Visible = true;
            this.Height += 50;
        }

        private void button3_Click(object sender, EventArgs e)
        {
            status = 2;
            this.button3.Visible = false;
            this.label7.Visible = true;
            this.textBox7.Visible = true;
            this.label8.Visible = true;
            this.textBox8.Visible = true;
            this.Height += 50;
        }
    }
}
