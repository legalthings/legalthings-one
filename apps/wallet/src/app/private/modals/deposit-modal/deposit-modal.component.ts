import { Component, OnInit } from '@angular/core';
import { toPromise } from '../../../core/utils';
import { AuthService, BridgeService } from '../../../core';
import { MatSnackBar } from '@angular/material';

@Component({
  selector: 'lto-deposit-modal',
  templateUrl: './deposit-modal.component.html',
  styleUrls: ['./deposit-modal.component.scss']
})
export class DepositModalComponent implements OnInit {
  bridgeAddress = '';
  error = false;
  loaded = false;

  constructor(
    private auth: AuthService,
    private bridgeService: BridgeService,
    private snackbar: MatSnackBar
  ) {
    this.generateBridgeAddress();
  }

  ngOnInit() {}

  async generateBridgeAddress() {
    try {
      this.bridgeAddress = await toPromise(this.bridgeService.depositTo(this.auth.wallet.address));
      this.loaded = true;
    } catch (error) {
      this.error = true;
      this.snackbar.open('Unable generate bridge', 'Dismiss', { duration: 3000 });
    } finally {
      this.loaded = true;
    }
  }
}
