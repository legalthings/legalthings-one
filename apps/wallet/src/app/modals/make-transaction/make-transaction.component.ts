import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material';
import { MyWallet } from '../../core';

@Component({
  selector: 'lto-wallet-make-transaction',
  templateUrl: './make-transaction.component.html',
  styleUrls: ['./make-transaction.component.scss']
})
export class MakeTransactionComponent implements OnInit {
  sendForm: FormGroup;

  constructor(public wallet: MyWallet, public dialog: MatDialogRef<any>) {
    this.sendForm = new FormGroup({
      recipient: new FormControl('', [Validators.required]),
      amount: new FormControl(0, [Validators.required]),
      attachment: new FormControl('', []),
      fee: new FormControl({ value: 0.001, disabled: true }, [Validators.required])
    });
  }

  ngOnInit() {}

  async send() {
    const { amount, fee, attachment, recipient } = this.sendForm.value;
    await this.wallet.transfer({ recipient, amount, fee, attachment });
    this.dialog.close();
  }
}
