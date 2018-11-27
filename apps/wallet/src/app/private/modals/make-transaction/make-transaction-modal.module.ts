import { NgModule } from '@angular/core';
import { SharedModule } from '../../../shared';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule } from '@angular/material';

import { MakeTransactionComponent } from './make-transaction.component';
import { MakeTransactionModal } from './make-transaction-modal';

@NgModule({
  imports: [SharedModule, MatDialogModule, ReactiveFormsModule],
  declarations: [MakeTransactionComponent],
  entryComponents: [MakeTransactionComponent],
  providers: [MakeTransactionModal]
})
export class MakeTransactionModalModule {}
